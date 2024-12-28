#include "objspace_coordinator.h"
#include "internal/numeric.h"
#include "ractor_core.h"
#include "vm_core.h"
#include "vm_sync.h"
#include "vm_callinfo.h"


/*
  ------------------------ Global Coordinator ------------------------
*/

struct rb_objspace *
gc_current_objspace(void)
{
    if (ruby_single_main_objspace) {
	return ruby_single_main_objspace;
    }

    rb_objspace_gate_t *os_gate = GET_OBJSPACE_GATE();
#ifdef RB_THREAD_LOCAL_SPECIFIER
    VM_ASSERT(os_gate == rb_current_os_gate_noinline());
#endif

    if (os_gate) {
	return os_gate->gc_target;
    }
    return GET_RACTOR()->local_objspace;
}

rb_objspace_coordinator_t *
rb_get_objspace_coordinator(void)
{
    return GET_VM()->objspace_coordinator;
}

rb_objspace_coordinator_t *
rb_objspace_coordinator_init(void)
{
    rb_objspace_coordinator_t *objspace_coordinator = calloc(1, sizeof(rb_objspace_coordinator_t));

    objspace_coordinator->next_object_id = OBJ_ID_INITIAL;
    objspace_coordinator->prev_id_assigner = NULL;

    rb_nativethread_lock_initialize(&objspace_coordinator->next_object_id_lock);
    rb_nativethread_lock_initialize(&objspace_coordinator->absorbed_thread_tbl_lock);
    rb_nativethread_lock_initialize(&objspace_coordinator->rglobalgc.shared_tracking_lock);

    rb_nativethread_lock_initialize(&objspace_coordinator->absorption.mode_lock);
    rb_native_cond_initialize(&objspace_coordinator->absorption.mode_change_cond);

    objspace_coordinator->absorbed_thread_tbl = st_init_numtable();

    objspace_coordinator->absorption.objspace_readers = 0;
    objspace_coordinator->absorption.absorbers = 0;

    objspace_coordinator->global_gc_underway = false;

    rb_native_cond_initialize(&objspace_coordinator->global_gc_finished);

    rb_vm_t *vm = GET_VM();
    vm->objspace_coordinator = objspace_coordinator;
    return objspace_coordinator;
}

void
rb_objspace_coordinator_free(rb_objspace_coordinator_t *objspace_coordinator)
{
    rb_nativethread_lock_destroy(&objspace_coordinator->next_object_id_lock);
    st_free_table(objspace_coordinator->absorbed_thread_tbl);
    rb_nativethread_lock_destroy(&objspace_coordinator->absorbed_thread_tbl_lock);
    rb_nativethread_lock_destroy(&objspace_coordinator->rglobalgc.shared_tracking_lock);

    rb_nativethread_lock_destroy(&objspace_coordinator->absorption.mode_lock);
    rb_native_cond_destroy(&objspace_coordinator->absorption.mode_change_cond);

    rb_vm_t *vm = GET_VM();
    vm->objspace_coordinator = NULL;

    free(objspace_coordinator);
}

VALUE
object_id_global_search(VALUE objid)
{
    struct rb_objspace *objspace = GET_RACTOR()->local_objspace;

    VALUE result = rb_objspace_object_id_local_search(objspace, objid);
    if (result != Qundef) {
	return result;
    }

    rb_vm_t *vm = GET_VM();

    lock_ractor_set();
    rb_objspace_gate_t *local_gate = NULL;
    ccan_list_for_each(&vm->objspace_set, local_gate, gate_node) {
	struct rb_objspace *os = local_gate->objspace;
	result = rb_objspace_object_id_local_search(os, objid);
	if (result != Qundef) {
	    break;
	}
    }
    unlock_ractor_set();
    return result;
}

bool
rb_nonexistent_id(VALUE objid)
{
    rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&objspace_coordinator->next_object_id_lock);
    VALUE not_id_value = rb_int_ge(objid, ULL2NUM(objspace_coordinator->next_object_id));
    rb_native_mutex_unlock(&objspace_coordinator->next_object_id_lock);
    return !!not_id_value;
}

VALUE
retrieve_next_obj_id(int increment)
{
    VALUE id;
    rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&objspace_coordinator->next_object_id_lock);
    id = ULL2NUM(objspace_coordinator->next_object_id);
    objspace_coordinator->next_object_id += increment;
    objspace_coordinator->prev_id_assigner = rb_current_allocating_ractor();
    rb_native_mutex_unlock(&objspace_coordinator->next_object_id_lock);
    return id;
}

void
rb_objspace_free_all_non_main(rb_vm_t *vm)
{
    rb_objspace_gate_t *local_gate = NULL, *next = NULL;
    ccan_list_for_each_safe(&GET_VM()->objspace_set, local_gate, next, gate_node) {
	if (local_gate->objspace != vm->objspace) {
	    rb_objspace_free(local_gate->objspace);
	}
    }
}

void
rb_ractor_sched_signal_possible_waiters(rb_objspace_coordinator_t *coordinator)
{
    rb_native_cond_broadcast(&coordinator->global_gc_finished);

    rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
    rb_native_cond_broadcast(&objspace_coordinator->absorption.mode_change_cond);

    rb_ractor_t *r = NULL;
    ccan_list_for_each(&GET_VM()->ractor.set, r, vmlr_node) {
	rb_native_cond_signal(&r->sync.close_cond);
	rb_native_cond_signal(&r->borrowing_sync.no_borrowers);
    }
}

void
rb_barrier_join_within_vm_lock(rb_vm_t *vm, rb_ractor_t *cr)
{
    RUBY_DEBUG_LOG("barrier serial:%u", vm->ractor.sched.barrier_serial);
    int lock_rec = vm->ractor.sync.lock_rec;
    rb_ractor_t *lock_owner = vm->ractor.sync.lock_owner;
    vm->ractor.sync.lock_rec = 0;
    vm->ractor.sync.lock_owner = NULL;
    rb_ractor_sched_barrier_join(vm, cr);
    vm->ractor.sync.lock_rec = lock_rec;
    vm->ractor.sync.lock_owner = lock_owner;
}

#if VM_CHECK_MODE > 0
int
count_objspaces(rb_vm_t *vm) //TODO: Replace with count-tracker
{
    int i = 0;
    rb_objspace_gate_t *local_gate = NULL;
    ccan_list_for_each(&vm->objspace_set, local_gate, gate_node) {
	i++;
    }
    return i;
}
#endif


/*
  ------------------------ Objspace Gate Data ------------------------
*/

static void mark_shared_reference_tbl(rb_objspace_gate_t *os_gate);
static void mark_local_immune_tbl(rb_objspace_gate_t *os_gate);
static void mark_received_obj_tbl(rb_objspace_gate_t *os_gate);

static void
objspace_gate_mark(void *data)
{
    rb_objspace_gate_t *os_gate = data;

    mark_objspace_cc_cache_table(os_gate);
    mark_contained_ractor_tbl(os_gate);
    mark_zombie_threads(os_gate);
    mark_absorbed_threads_tbl(os_gate);

    if (rb_using_local_limits(rb_gc_get_objspace()) || !rb_during_gc()) {
	mark_shared_reference_tbl(os_gate);
	mark_received_obj_tbl(os_gate);
	mark_local_immune_tbl(os_gate);
    }
}

static void
objspace_gate_free(rb_objspace_gate_t *local_gate)
{
    if (!local_gate->objspace_closed) {
	lock_ractor_set();
	ccan_list_del(&local_gate->gate_node);
	unlock_ractor_set();
    }

    st_free_table(local_gate->shared_reference_tbl);
    rb_nativethread_lock_destroy(&local_gate->shared_reference_tbl_lock);
    st_free_table(local_gate->external_reference_tbl);
    rb_nativethread_lock_destroy(&local_gate->external_reference_tbl_lock);
    st_free_table(local_gate->local_immune_tbl);
    rb_nativethread_lock_destroy(&local_gate->local_immune_tbl_lock);

    st_free_table(local_gate->received_obj_tbl);
    rb_nativethread_lock_destroy(&local_gate->received_obj_tbl_lock);

    st_free_table(local_gate->wmap_referenced_obj_tbl);
    rb_nativethread_lock_destroy(&local_gate->wmap_referenced_obj_tbl_lock);

    st_free_table(local_gate->contained_ractor_tbl);
    rb_nativethread_lock_destroy(&local_gate->contained_ractor_tbl_lock);

    rb_nativethread_lock_destroy(&local_gate->zombie_threads_lock);

    rb_nativethread_lock_destroy(&local_gate->objspace_lock);

    rb_nativethread_lock_destroy(&local_gate->running_local_gc_lock);
    rb_native_cond_destroy(&local_gate->local_gc_stop_cond);

    if (!local_gate->objspace_closed && local_gate->ractor->local_gate == local_gate) {
	local_gate->ractor = NULL;
    }
    if (GET_OBJSPACE_GATE() == local_gate) {
	rb_ractor_set_current_os_gate(NULL);
    }
    
    free(local_gate);
}

static size_t
rb_objspace_gate_memsize(const void *data)
{
    //TODO memsize
    return 0;
}

static const rb_data_type_t objspace_gate_data_type = {
    "objspace_gate",
    {
        objspace_gate_mark,
        objspace_gate_free,
        rb_objspace_gate_memsize,
        NULL,
    },
    0,0, RUBY_TYPED_FREE_IMMEDIATELY,
};

static VALUE
objspace_gate_object_create(VALUE arg)
{
    rb_objspace_gate_t *os_gate = (rb_objspace_gate_t *)arg;
    os_gate->self = TypedData_Wrap_Struct(0, &objspace_gate_data_type, os_gate);
    return Qnil;
}

rb_objspace_gate_t *
rb_objspace_gate_init(struct rb_objspace *objspace)
{
    rb_objspace_gate_t *local_gate = calloc(1, sizeof(rb_objspace_gate_t));

    rb_gc_attach_local_gate(objspace, local_gate);

    local_gate->objspace = local_gate->gc_target = objspace;
    local_gate->ractor = rb_gc_ractor_of_objspace(objspace);
    local_gate->local_gc_level = 0;

    local_gate->ractor->local_gate = local_gate;

    rb_vm_t *vm = GET_VM();
    if (objspace != vm->objspace) {
	ruby_single_main_objspace = NULL;
	rb_objspace_gate_t *main_gate = vm->main_os_gate;
	main_gate->belong_to_single_main_ractor = false;
    }

    ccan_list_head_init(&local_gate->zombie_threads);

    rb_nativethread_lock_initialize(&local_gate->zombie_threads_lock);

    local_gate->shared_reference_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->shared_reference_tbl_lock);
    local_gate->external_reference_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->external_reference_tbl_lock);
    
    local_gate->local_immune_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->local_immune_tbl_lock);
    
    local_gate->received_obj_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->received_obj_tbl_lock);

    local_gate->wmap_referenced_obj_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->wmap_referenced_obj_tbl_lock);

    local_gate->contained_ractor_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_gate->contained_ractor_tbl_lock);

    rb_nativethread_lock_initialize(&local_gate->objspace_lock);
    local_gate->objspace_lock_owner = NULL;
    local_gate->objspace_lock_level = 0;

    local_gate->running_local_gc = false;
    rb_nativethread_lock_initialize(&local_gate->running_local_gc_lock);
    rb_native_cond_initialize(&local_gate->local_gc_stop_cond);

    local_gate->alloc_target_ractor = NULL;

    lock_ractor_set();
    ccan_list_add_tail(&vm->objspace_set, &local_gate->gate_node);
    unlock_ractor_set();
    if (local_gate->objspace == ruby_single_main_objspace) {
	objspace_gate_object_create(local_gate);
    }
    else {
	rb_run_with_redirected_allocation(local_gate->ractor, objspace_gate_object_create, local_gate);
    }
    return local_gate;
}

bool
rb_obj_is_main_os_gate(VALUE obj)
{
    return GET_VM()->main_os_gate->self == obj;
}

int
st_insert_no_gc(st_table *tab, st_data_t key, st_data_t value)
{
    VALUE already_disabled = rb_gc_disable_no_rest();
    int ret = st_insert(tab, key, value);
    if (already_disabled == Qfalse) rb_gc_enable();
    return ret;
}

static void *
st_lookup_or_null(st_table *tbl, VALUE obj)
{
    st_data_t data;
    bool found = !!st_lookup(tbl, (st_data_t)obj, &data);
    if (found) {
	return (void *)data;
    }
    else {
	return NULL;
    }
}

static void
st_delete_and_free_value(st_table *tbl, VALUE obj)
{
    st_data_t rs;
    st_delete(tbl, (st_data_t *)&obj, &rs);
    free(rs);
}

static void
add_external_reference_usage(rb_objspace_gate_t *os_gate, VALUE obj, gc_reference_status_t *rs)
{
    WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
    {
	rb_native_mutex_lock(&source_gate->shared_reference_tbl_lock);
	gc_reference_status_t *local_rs = st_lookup_or_null(source_gate->shared_reference_tbl, obj);

	if (local_rs) {
	    ATOMIC_INC(*local_rs->refcount);
	}
	else {
	    VM_ASSERT(rb_during_gc() || (rb_current_allocating_ractor() == source_gate->ractor) || (rb_current_allocating_ractor() == os_gate->ractor));

	    rb_atomic_t *refcount = malloc(sizeof(rb_atomic_t));
	    *refcount = 1;

	    local_rs = malloc(sizeof(gc_reference_status_t));
	    local_rs->refcount = refcount;
	    local_rs->status = shared_object_local;
	    st_insert_no_gc(source_gate->shared_reference_tbl, obj, local_rs);

	    rb_objspace_coordinator_t *coordinator = rb_get_objspace_coordinator();
	    rb_native_mutex_lock(&coordinator->rglobalgc.shared_tracking_lock);
	    coordinator->rglobalgc.shared_objects_total++;
	    rb_native_mutex_unlock(&coordinator->rglobalgc.shared_tracking_lock);
	}
	rb_native_mutex_unlock(&source_gate->shared_reference_tbl_lock);
	rs->refcount = local_rs->refcount;
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

static void
drop_external_reference_usage(rb_objspace_gate_t *os_gate, VALUE obj, gc_reference_status_t *rs)
{
    rb_atomic_t *refcount = rs->refcount;
    rb_atomic_t prev_count = RUBY_ATOMIC_FETCH_SUB(*refcount, 1);
    if (prev_count == 1) {
	WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
	{
	    rb_native_mutex_lock(&source_gate->shared_reference_tbl_lock);
	    if (*refcount == 0) {
		st_delete_and_free_value(source_gate->shared_reference_tbl, obj);
		free(refcount);

		rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
		rb_native_mutex_lock(&objspace_coordinator->rglobalgc.shared_tracking_lock);
		objspace_coordinator->rglobalgc.shared_objects_total--;
		rb_native_mutex_unlock(&objspace_coordinator->rglobalgc.shared_tracking_lock);
	    }
	    rb_native_mutex_unlock(&source_gate->shared_reference_tbl_lock);
	}
	WITH_OBJSPACE_GATE_LEAVE(source_gate);
    }
    free(rs);
}

static void
register_new_external_reference(rb_objspace_gate_t *receiving_gate, rb_objspace_gate_t *source_gate, VALUE obj)
{
    VM_ASSERT(!RB_SPECIAL_CONST_P(obj));
    VM_ASSERT(GET_OBJSPACE_OF_VALUE(obj) == source_gate->objspace);
    VM_ASSERT(receiving_gate != source_gate);

    rb_native_mutex_lock(&receiving_gate->external_reference_tbl_lock);

    gc_reference_status_t *rs = st_lookup_or_null(receiving_gate->external_reference_tbl, obj);
    bool new_addition = !rs;

    if (new_addition) {
	gc_reference_status_t *rs = malloc(sizeof(gc_reference_status_t));

	add_external_reference_usage(receiving_gate, obj, rs);

	if (receiving_gate == rb_current_allocating_ractor()->local_gate) {
	    rs->status = shared_object_unmarked;
	}
	else {
	    rs->status = shared_object_added_externally;
	}
	st_insert_no_gc(receiving_gate->external_reference_tbl, obj, rs);
    }

    rb_native_mutex_unlock(&receiving_gate->external_reference_tbl_lock);
}

void
rb_register_new_external_reference(rb_objspace_gate_t *receiving_gate, VALUE obj)
{
    if (RB_SPECIAL_CONST_P(obj)) return;
    WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
    {
	if (source_gate != receiving_gate) register_new_external_reference(receiving_gate, source_gate, obj);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

static void
confirm_externally_added_external_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    VALUE obj = (VALUE)key;
    if (rs->status == shared_object_added_externally) {
	rs->status = shared_object_unmarked;
    }
}

void
confirm_externally_added_external_references(rb_objspace_gate_t *local_gate)
{
    rb_native_mutex_lock(&local_gate->external_reference_tbl_lock);
    st_foreach(local_gate->external_reference_tbl, confirm_externally_added_external_references_i, NULL);
    rb_native_mutex_unlock(&local_gate->external_reference_tbl_lock);
}

static int
external_references_none_marked_i(st_data_t key, st_data_t value, st_data_t arg)
{
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    if (rs->status != shared_object_marked && rs->status != shared_object_discovered_and_marked) {
	return ST_CONTINUE;
    }
    else {
	bool *none_marked = (bool *)arg;
	*none_marked = false;
	return ST_STOP;
    }
}

static bool
external_references_none_marked(rb_objspace_gate_t *local_gate)
{
    bool none_marked = true;
    rb_native_mutex_lock(&local_gate->external_reference_tbl_lock);
    st_foreach(local_gate->external_reference_tbl, external_references_none_marked_i, (st_data_t)&none_marked);
    rb_native_mutex_unlock(&local_gate->external_reference_tbl_lock);
    return none_marked;
}

void
mark_in_external_reference_tbl(rb_objspace_gate_t *os_gate, VALUE obj)
{
    if (os_gate->marking_machine_context) return;
    rb_native_mutex_lock(&os_gate->external_reference_tbl_lock);
    gc_reference_status_t *rs = st_lookup_or_null(os_gate->external_reference_tbl, obj);
    if (rs) {
	if (rs->status == shared_object_unmarked) {
	    rs->status = shared_object_marked;
	}
    }
    else {
	rs = malloc(sizeof(gc_reference_status_t));
	rs->refcount = NULL;
	rs->status = shared_object_discovered_and_marked;
	st_insert_no_gc(os_gate->external_reference_tbl, obj, rs);
    }
    rb_native_mutex_unlock(&os_gate->external_reference_tbl_lock);
}

bool
rb_external_reference_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj)
{
    return !!st_lookup(os_gate->external_reference_tbl, obj, NULL);
}

static int
shared_references_all_marked_i(st_data_t key, st_data_t value, st_data_t arg)
{
    if (!rb_gc_object_marked((VALUE)key)) {
	bool *all_marked = (bool *)arg;
	*all_marked = false;
	return ST_STOP;
    }
    return ST_CONTINUE;
}

static bool
shared_references_all_marked(rb_objspace_gate_t *os_gate)
{
    bool all_marked = true;

    rb_native_mutex_lock(&os_gate->shared_reference_tbl_lock);
    st_foreach(os_gate->shared_reference_tbl, shared_references_all_marked_i, (st_data_t)&all_marked);
    rb_native_mutex_unlock(&os_gate->shared_reference_tbl_lock);
    return all_marked;
}

static void
mark_shared_reference_tbl(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->shared_reference_tbl_lock);
    rb_mark_set(os_gate->shared_reference_tbl);
    rb_native_mutex_unlock(&os_gate->shared_reference_tbl_lock);
}

bool
rb_shared_reference_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj)
{
    return !!st_lookup(os_gate->shared_reference_tbl, obj, NULL);
}

void
add_local_immune_object(VALUE obj)
{
    WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
    {
	if (rb_ractor_shareable_p(obj)) {
	    rb_native_mutex_lock(&source_gate->local_immune_tbl_lock);
	    bool new_entry = !st_insert_no_gc(source_gate->local_immune_tbl, (st_data_t)obj, INT2FIX(0));
	    if (new_entry) source_gate->local_immune_count++;
	    rb_native_mutex_unlock(&source_gate->local_immune_tbl_lock);
	}
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

static void
mark_local_immune_tbl(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->local_immune_tbl_lock);
    rb_mark_set(os_gate->local_immune_tbl);
    rb_native_mutex_unlock(&os_gate->local_immune_tbl_lock);
}

static int
update_local_immune_tbl_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    if (rb_gc_object_marked(key)) {
	return ST_CONTINUE;
    }
    rb_objspace_gate_t *os_gate = argp;
    os_gate->local_immune_count--;
    return ST_DELETE;
}

void
update_local_immune_tbl(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->local_immune_tbl_lock);
    st_foreach(os_gate->local_immune_tbl, update_local_immune_tbl_i, (st_data_t)os_gate);
    rb_native_mutex_unlock(&os_gate->local_immune_tbl_lock);
}

bool
rb_local_immune_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj, bool lock_needed)
{
    bool ret;
    if (lock_needed) {
	rb_native_mutex_lock(&os_gate->local_immune_tbl_lock);
	ret = !!st_lookup(os_gate->local_immune_tbl, obj, NULL);
	rb_native_mutex_unlock(&os_gate->local_immune_tbl_lock);
    }
    else {
	ret = !!st_lookup(os_gate->local_immune_tbl, obj, NULL);
    }
    return ret;
}

unsigned int
local_immune_objects_global_count(void)
{
    rb_objspace_gate_t *local_gate = NULL;
    unsigned int total = 0;
    ccan_list_for_each(&GET_VM()->objspace_set, local_gate, gate_node) {
	rb_native_mutex_lock(&local_gate->local_immune_tbl_lock);
	total += local_gate->local_immune_count;
	rb_native_mutex_unlock(&local_gate->local_immune_tbl_lock);
    }
    return total;
}

static int
find_all_mutable_shareable_objs_i(void *vstart, void *vend, size_t stride, void *data)
{
    VALUE ary = data;

    VALUE v = (VALUE)vstart;
    for (; v != (VALUE)vend; v += stride) {
	if (MUTABLE_SHAREABLE(v)) {
	    rb_ary_push(ary, v);
	}
    }

    return 0;
}

static VALUE
find_all_mutable_shareable_objs(void)
{
    VALUE ary = rb_ary_new();
    rb_objspace_each_objects(find_all_mutable_shareable_objs_i, ary);
    return ary;
}

void
add_reachable_objects_to_local_immune_tbl_i(VALUE obj, void *data_ptr)
{
    if (rb_ractor_shareable_p(obj)) {
	add_local_immune_object(obj);
    }
}

static void
add_reachable_objects_to_local_immune_tbl(VALUE obj)
{
    rb_objspace_reachable_objects_from(obj, add_reachable_objects_to_local_immune_tbl_i, NULL);
}

void
rb_local_immune_tbl_activate(void)
{
    VALUE mutable_shareable_obj_ary = find_all_mutable_shareable_objs();
    for (long i=0; i<RARRAY_LEN(mutable_shareable_obj_ary); i++) {
        add_reachable_objects_to_local_immune_tbl(RARRAY_AREF(mutable_shareable_obj_ary, i));
    }
}

static int
confirm_discovered_external_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    rb_objspace_gate_t *os_gate = (rb_objspace_gate_t *)argp;
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    VALUE obj = (VALUE)key;
    if (rs->status == shared_object_discovered_and_marked) {
	add_external_reference_usage(os_gate, obj, rs);
	rs->status = shared_object_marked;
    }
    return ST_CONTINUE;
}

static void
confirm_discovered_external_references(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->external_reference_tbl_lock);
    st_foreach(os_gate->external_reference_tbl, confirm_discovered_external_references_i, (st_data_t)os_gate);
    rb_native_mutex_unlock(&os_gate->external_reference_tbl_lock);
}


static bool local_limits_in_use(rb_objspace_gate_t *os_gate);

static int
keep_marked_shared_object_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    rb_objspace_gate_t *os_gate = (struct rb_objspace *)argp;
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    VALUE obj = (VALUE)key;
    switch (rs->status) {
	case shared_object_unmarked:
	    drop_external_reference_usage(os_gate, obj, rs);
	    return ST_DELETE;
	case shared_object_marked:
	    VM_ASSERT(local_limits_in_use(os_gate) || rb_gc_object_marked(obj));
	    rs->status = shared_object_unmarked;
	    return ST_CONTINUE;
	case shared_object_added_externally:
	    if (LIKELY(local_limits_in_use(os_gate) || rb_gc_object_marked(obj))) {
		rs->status = shared_object_unmarked;
		return ST_CONTINUE;
	    }
	    else {
		drop_external_reference_usage(os_gate, obj, rs);
		return ST_DELETE;
	    }
	default:
	    rb_bug("update_shared_object_references_i: unreachable");
    }
}

static void
keep_marked_shared_object_references(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->external_reference_tbl_lock);
    st_foreach(os_gate->external_reference_tbl, keep_marked_shared_object_references_i, (st_data_t)os_gate);
    rb_native_mutex_unlock(&os_gate->external_reference_tbl_lock);
}

void
update_shared_object_references(rb_objspace_gate_t *os_gate)
{
    confirm_discovered_external_references(os_gate);
    keep_marked_shared_object_references(os_gate);
    VM_ASSERT(external_references_none_marked(os_gate));
}

#if VM_CHECK_MODE > 0
bool
shared_reference_tbl_empty(rb_objspace_gate_t *os_gate)
{
    bool empty;
    rb_native_mutex_lock(&os_gate->shared_reference_tbl_lock);
    empty = (st_table_size(os_gate->shared_reference_tbl) == 0);
    rb_native_mutex_unlock(&os_gate->shared_reference_tbl_lock);
    return empty;
}

bool
external_reference_tbl_empty(rb_objspace_gate_t *os_gate)
{
    bool empty;
    rb_native_mutex_lock(&os_gate->external_reference_tbl_lock);
    empty = (st_table_size(os_gate->external_reference_tbl) == 0);
    rb_native_mutex_unlock(&os_gate->external_reference_tbl_lock);
    return empty;
}

#endif

void
rb_add_zombie_thread(rb_thread_t *th)
{
    rb_vm_t *vm = th->vm;
    th->sched.finished = false;

    WITH_OBJSPACE_GATE_ENTER(th->self, source_gate);
    {
	rb_native_mutex_lock(&source_gate->zombie_threads_lock);
	ccan_list_add(&source_gate->zombie_threads, &th->sched.node.zombie_threads);
	rb_native_mutex_unlock(&source_gate->zombie_threads_lock);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

void
mark_zombie_threads(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->zombie_threads_lock);
    if (!ccan_list_empty(&os_gate->zombie_threads)) {
        rb_thread_t *zombie_th, *next_zombie_th;
        ccan_list_for_each_safe(&os_gate->zombie_threads, zombie_th, next_zombie_th, sched.node.zombie_threads) {
            if (zombie_th->sched.finished) {
                ccan_list_del_init(&zombie_th->sched.node.zombie_threads);
            }
            else {
                rb_gc_mark(zombie_th->self);
            }
        }
    }
    rb_native_mutex_unlock(&os_gate->zombie_threads_lock);
}

void
rb_add_to_contained_ractor_tbl(rb_ractor_t *r)
{
    VALUE ractor_obj = r->pub.self;
    WITH_OBJSPACE_GATE_ENTER(ractor_obj, source_gate);
    {
	rb_native_mutex_lock(&source_gate->contained_ractor_tbl_lock);
	st_insert_no_gc(source_gate->contained_ractor_tbl, (st_data_t)ractor_obj, INT2FIX(0));
	rb_native_mutex_unlock(&source_gate->contained_ractor_tbl_lock);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

void
rb_remove_from_contained_ractor_tbl(rb_ractor_t *r)
{
    VALUE ractor_obj = r->pub.self;
    WITH_OBJSPACE_GATE_ENTER(ractor_obj, source_gate);
    {
	rb_native_mutex_lock(&source_gate->contained_ractor_tbl_lock);
	st_delete(source_gate->contained_ractor_tbl, (st_data_t *) &ractor_obj, NULL);
	rb_native_mutex_unlock(&source_gate->contained_ractor_tbl_lock);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

void
mark_contained_ractor_tbl(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->contained_ractor_tbl_lock);
    rb_mark_set(os_gate->contained_ractor_tbl);
    rb_native_mutex_unlock(&os_gate->contained_ractor_tbl_lock);
}

struct received_obj_list {
    VALUE received_obj;
    struct received_obj_list *next;
};

void
rb_register_received_obj(rb_objspace_gate_t *os_gate, uintptr_t borrowing_id, VALUE obj)
{
    rb_native_mutex_lock(&os_gate->received_obj_tbl_lock);
    struct received_obj_list *new_item;
    new_item = ALLOC(struct received_obj_list);
    new_item->received_obj = obj;

    st_data_t data;
    int list_found = st_lookup(os_gate->received_obj_tbl, (st_data_t)borrowing_id, &data);
    struct received_obj_list *lst = list_found ? (struct received_obj_list *)data : NULL;
    new_item->next = lst;

    st_insert_no_gc(os_gate->received_obj_tbl, (st_data_t)GET_RACTOR()->borrowing_sync.borrowing_id, (st_data_t)new_item);
    rb_native_mutex_unlock(&os_gate->received_obj_tbl_lock);
}

static void
remove_received_obj_list(rb_objspace_gate_t *os_gate, uintptr_t borrowing_id)
{
    rb_native_mutex_lock(&os_gate->received_obj_tbl_lock);
    st_data_t data;
    int list_found = st_lookup(os_gate->received_obj_tbl, (st_data_t)borrowing_id, &data);
    if (list_found) {
	struct received_obj_list *p = (struct received_obj_list *)data;
	struct received_obj_list *next = NULL;
	while (p) {
	    next = p->next;
	    free(p);
	    p = next;
	}
    }

    st_delete(os_gate->received_obj_tbl, &borrowing_id, NULL);
    rb_native_mutex_unlock(&os_gate->received_obj_tbl_lock);
}

static int
mark_received_obj_list(st_data_t key, st_data_t val, st_data_t arg)
{
    struct received_obj_list *p = (struct received_obj_list *)val;
    while (p) {
	rb_gc_mark(p->received_obj);
	p = p->next;
    }
    return ST_CONTINUE;
}

static void
mark_received_obj_tbl(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->received_obj_tbl_lock);
    st_foreach(os_gate->received_obj_tbl, mark_received_obj_list, NULL);
    rb_native_mutex_unlock(&os_gate->received_obj_tbl_lock);
}

void
rb_register_new_external_wmap_reference(VALUE *ptr)
{
    VALUE obj = *ptr;
    if (SPECIAL_CONST_P(obj)) return;

    WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
    {
	rb_native_mutex_lock(&source_gate->wmap_referenced_obj_tbl_lock);
	st_insert_no_gc(source_gate->wmap_referenced_obj_tbl, ptr, INT2FIX(0));
	rb_native_mutex_unlock(&source_gate->wmap_referenced_obj_tbl_lock);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

void
rb_remove_from_external_weak_tables(VALUE *ptr)
{
    VALUE obj = *ptr;
    if (obj == Qundef || SPECIAL_CONST_P(obj) || !FL_TEST_RAW(obj, FL_SHAREABLE)) return;

    WITH_OBJSPACE_GATE_ENTER(obj, source_gate);
    {
	rb_native_mutex_lock(&source_gate->wmap_referenced_obj_tbl_lock);
	st_delete(source_gate->wmap_referenced_obj_tbl, &ptr, NULL);
	rb_native_mutex_unlock(&source_gate->wmap_referenced_obj_tbl_lock);
    }
    WITH_OBJSPACE_GATE_LEAVE(source_gate);
}

static int
update_external_weak_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    VALUE *ptr = (VALUE *)key;
    VALUE obj = *ptr;
    if (obj == Qundef) {
	return ST_DELETE;
    }
    else if (rb_gc_object_marked(obj)) {
	return ST_CONTINUE;
    }
    else {
	*ptr = Qundef;
	return ST_DELETE;
    }
}

void
gc_update_external_weak_references(rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->wmap_referenced_obj_tbl_lock);
    st_foreach(os_gate->wmap_referenced_obj_tbl, update_external_weak_references_i, NULL);
    rb_native_mutex_unlock(&os_gate->wmap_referenced_obj_tbl_lock);
}

const struct rb_callcache *
get_from_objspace_cc_cache_table(int index)
{
    return GET_OBJSPACE_GATE()->objspace_cc_cache_table[index];
}

void
set_in_objspace_cc_cache_table(int index, const struct rb_callcache *cc)
{
    GET_OBJSPACE_GATE()->objspace_cc_cache_table[index] = cc;
}

static void
mark_objspace_cc_cache_table_item(rb_objspace_gate_t *os_gate, int index)
{
    const struct rb_callcache *cc = os_gate->objspace_cc_cache_table[index];

    if (cc != NULL) {
	if (!vm_cc_invalidated_p(cc)) {
	    rb_gc_mark((VALUE)cc);
	}
	else {
	    os_gate->objspace_cc_cache_table[index] = NULL;
	}
    }
}

void
mark_objspace_cc_cache_table(rb_objspace_gate_t *os_gate)
{
    for (int i=0; i<VM_OBJSPACE_CC_CACHE_TABLE_SIZE; i++) {
	mark_objspace_cc_cache_table_item(os_gate, i);
    }
}


/*
  ------------------------ Absorption ------------------------
*/

static void
objspace_absorb_enter(rb_objspace_coordinator_t *coordinator)
{
    rb_native_mutex_lock(&coordinator->absorption.mode_lock);

    ASSERT_vm_locking();
    VM_ASSERT(coordinator->absorption.objspace_readers == 0 || coordinator->absorption.absorbers == 0);

    COND_AND_BARRIER_WAIT(GET_VM(), coordinator->absorption.mode_lock, coordinator->absorption.mode_change_cond, coordinator->absorption.objspace_readers == 0);

    coordinator->absorption.absorbers++;
    rb_native_mutex_unlock(&coordinator->absorption.mode_lock);
}

static void
objspace_absorb_leave(rb_objspace_coordinator_t *coordinator)
{
    rb_native_mutex_lock(&coordinator->absorption.mode_lock);

    VM_ASSERT(coordinator->absorption.objspace_readers == 0 || coordinator->absorption.absorbers == 0);

    coordinator->absorption.absorbers--;
    if (coordinator->absorption.absorbers == 0) {
	rb_native_cond_broadcast(&coordinator->absorption.mode_change_cond);
    }

    rb_native_mutex_unlock(&coordinator->absorption.mode_lock);
}

static void
objspace_read_enter(rb_objspace_coordinator_t *coordinator)
{
    rb_native_mutex_lock(&coordinator->absorption.mode_lock);

    VM_ASSERT(coordinator->absorption.objspace_readers == 0 || coordinator->absorption.absorbers == 0);

    while (coordinator->absorption.absorbers > 0) {
	rb_native_cond_wait(&coordinator->absorption.mode_change_cond, &coordinator->absorption.mode_lock);
    }
    coordinator->absorption.objspace_readers++;
    rb_native_mutex_unlock(&coordinator->absorption.mode_lock);
}

void
rb_objspace_read_enter(rb_objspace_coordinator_t *coordinator)
{
    rb_objspace_gate_t *local_gate = GET_OBJSPACE_GATE();
    if (local_gate && local_gate->currently_absorbing) return;
    
    objspace_read_enter(coordinator);
}

static void
objspace_read_leave(rb_objspace_coordinator_t *coordinator)
{
    rb_native_mutex_lock(&coordinator->absorption.mode_lock);

    VM_ASSERT(coordinator->absorption.objspace_readers == 0 || coordinator->absorption.absorbers == 0);

    coordinator->absorption.objspace_readers--;
    if (coordinator->absorption.objspace_readers == 0) {
	rb_native_cond_broadcast(&coordinator->absorption.mode_change_cond);
    }
    rb_native_mutex_unlock(&coordinator->absorption.mode_lock);
}

void
rb_objspace_read_leave(rb_objspace_coordinator_t *coordinator)
{
    rb_objspace_gate_t *local_gate = GET_OBJSPACE_GATE();
    if (local_gate && local_gate->currently_absorbing) return;
    
    objspace_read_leave(coordinator);
}

static int
absorb_table_row(st_data_t key, st_data_t val, st_data_t arg)
{
    st_table *tbl = (st_table *)arg;
    st_insert_no_gc(tbl, key, val);
    return ST_CONTINUE;
}

void
absorb_table_contents(st_table *receiving_tbl, st_table *added_tbl)
{
    st_foreach(added_tbl, absorb_table_row, (st_data_t)receiving_tbl);
}

static int
insert_external_reference_row(st_data_t key, st_data_t val, st_data_t arg)
{
    VALUE obj = (VALUE)key;
    gc_reference_status_t *rs = (gc_reference_status_t *)val;
    rb_objspace_gate_t **local_gate = (rb_objspace_gate_t **)arg;
    rb_objspace_gate_t *gate_to_update = local_gate[0];
    rb_objspace_gate_t *gate_to_copy_from = local_gate[1];
    st_table *target_tbl = gate_to_update->external_reference_tbl;

    VM_ASSERT(GET_OBJSPACE_OF_VALUE(obj) != gate_to_update->objspace);
    bool replaced = !!st_insert(target_tbl, key, val);
    if (replaced) {
	ATOMIC_DEC(*rs->refcount);
    }
    return ST_CONTINUE;
}

static int
prepare_for_reference_tbl_absorption_i(st_data_t key, st_data_t val, st_data_t arg)
{
    VALUE obj = (VALUE)key;
    gc_reference_status_t *rs = (gc_reference_status_t *)val;
    rb_objspace_gate_t **objspaces = (rb_objspace_gate_t **)arg;

    rb_objspace_gate_t *table_owner_gate = objspaces[0];
    rb_objspace_gate_t *comparison_os_gate = objspaces[1];
    struct rb_objspace *source_objspace = GET_OBJSPACE_OF_VALUE(obj);

    if (source_objspace == comparison_os_gate->objspace) {
	drop_external_reference_usage(table_owner_gate, obj, rs);
	return ST_DELETE;
    }
    return ST_CONTINUE;
}

static void
prepare_for_reference_tbl_absorption(rb_objspace_gate_t *gate_to_update, rb_objspace_gate_t *gate_to_copy_from)
{
    rb_objspace_gate_t *objspaces[2];
    objspaces[0] = gate_to_copy_from;
    objspaces[1] = gate_to_update;
    st_foreach(gate_to_copy_from->external_reference_tbl, prepare_for_reference_tbl_absorption_i, (st_data_t)objspaces);

    objspaces[0] = gate_to_update;
    objspaces[1] = gate_to_copy_from;
    st_foreach(gate_to_update->external_reference_tbl, prepare_for_reference_tbl_absorption_i, (st_data_t)objspaces);
}

static void
absorb_external_references(rb_objspace_gate_t *gate_to_update, rb_objspace_gate_t *gate_to_copy_from)
{
    rb_objspace_gate_t *local_gate[2];
    local_gate[0] = gate_to_update;
    local_gate[1] = gate_to_copy_from;
    st_foreach(gate_to_copy_from->external_reference_tbl, insert_external_reference_row, (st_data_t)local_gate);
}

static void
absorb_shared_object_tables(rb_objspace_gate_t *gate_to_update, rb_objspace_gate_t *gate_to_copy_from)
{
    rb_native_mutex_lock(&gate_to_copy_from->external_reference_tbl_lock);
    rb_native_mutex_lock(&gate_to_update->external_reference_tbl_lock);

    prepare_for_reference_tbl_absorption(gate_to_update, gate_to_copy_from);
    absorb_external_references(gate_to_update, gate_to_copy_from);

    rb_native_mutex_unlock(&gate_to_copy_from->external_reference_tbl_lock);
    rb_native_mutex_unlock(&gate_to_update->external_reference_tbl_lock);

    rb_native_mutex_lock(&gate_to_copy_from->shared_reference_tbl_lock);
    rb_native_mutex_lock(&gate_to_update->shared_reference_tbl_lock);

    absorb_table_contents(gate_to_update->shared_reference_tbl, gate_to_copy_from->shared_reference_tbl);

    rb_native_mutex_unlock(&gate_to_copy_from->shared_reference_tbl_lock);
    rb_native_mutex_unlock(&gate_to_update->shared_reference_tbl_lock);

    rb_native_mutex_lock(&gate_to_copy_from->local_immune_tbl_lock);
    rb_native_mutex_lock(&gate_to_update->local_immune_tbl_lock);

    absorb_table_contents(gate_to_update->local_immune_tbl, gate_to_copy_from->local_immune_tbl);
    gate_to_update->local_immune_count += gate_to_copy_from->local_immune_count;

    rb_native_mutex_unlock(&gate_to_copy_from->local_immune_tbl_lock);
    rb_native_mutex_unlock(&gate_to_update->local_immune_tbl_lock);
}

static void
absorb_objspace_tables(rb_objspace_gate_t *gate_to_update, rb_objspace_gate_t *gate_to_copy_from)
{
    //Contained ractor table
    rb_native_mutex_lock(&gate_to_copy_from->contained_ractor_tbl_lock);
    rb_native_mutex_lock(&gate_to_update->contained_ractor_tbl_lock);

    absorb_table_contents(gate_to_update->contained_ractor_tbl, gate_to_copy_from->contained_ractor_tbl);

    rb_native_mutex_unlock(&gate_to_copy_from->contained_ractor_tbl_lock);
    rb_native_mutex_unlock(&gate_to_update->contained_ractor_tbl_lock);

    //Shared object tables
    absorb_shared_object_tables(gate_to_update, gate_to_copy_from);

    rb_native_mutex_lock(&gate_to_copy_from->wmap_referenced_obj_tbl_lock);
    rb_native_mutex_lock(&gate_to_update->wmap_referenced_obj_tbl_lock);

    absorb_table_contents(gate_to_update->wmap_referenced_obj_tbl, gate_to_copy_from->wmap_referenced_obj_tbl);

    rb_native_mutex_unlock(&gate_to_copy_from->wmap_referenced_obj_tbl_lock);
    rb_native_mutex_unlock(&gate_to_update->wmap_referenced_obj_tbl_lock);
}

static void
transfer_zombie_threads(rb_objspace_gate_t *receiving_gate, rb_objspace_gate_t *closing_gate)
{
    rb_native_mutex_lock(&receiving_gate->zombie_threads_lock);
    //TODO: Assert that the closing objspace has had its pages absorbed (so locking is not needed)

    if (!ccan_list_empty(&closing_gate->zombie_threads)) {
        rb_thread_t *zombie_th, *next_zombie_th;
        ccan_list_for_each_safe(&closing_gate->zombie_threads, zombie_th, next_zombie_th, sched.node.zombie_threads) {
	    ccan_list_del_init(&zombie_th->sched.node.zombie_threads);
	    ccan_list_add(&receiving_gate->zombie_threads, &zombie_th->sched.node.zombie_threads);
        }
    }

    rb_native_mutex_unlock(&receiving_gate->zombie_threads_lock);
}

static void
perform_absorption(rb_objspace_gate_t *receiving_gate, rb_objspace_gate_t *closing_gate)
{
    absorb_objspace_tables(receiving_gate, closing_gate);
    rb_objspace_absorb_contents(receiving_gate->objspace, closing_gate->objspace);
    transfer_zombie_threads(receiving_gate, closing_gate);
}

void
rb_absorb_objspace_of_closing_ractor(rb_ractor_t *receiving_ractor, rb_ractor_t *closing_ractor)
{ 
    rb_objspace_coordinator_t *coordinator = rb_get_objspace_coordinator();
    rb_objspace_gate_t *receiving_gate = receiving_ractor->local_gate;
    rb_objspace_gate_t *closing_gate = closing_ractor->local_gate;

    VALUE already_disabled = rb_objspace_gc_disable(receiving_gate->objspace);
    RB_VM_LOCK();
    {

	VM_COND_AND_BARRIER_WAIT(GET_VM(), closing_ractor->sync.close_cond, closing_ractor->sync.ready_to_close);

	objspace_absorb_enter(coordinator);

	receiving_gate->currently_absorbing = true;

	rb_borrowing_sync_lock(receiving_ractor);
	OBJSPACE_LOCK_ENTER(receiving_gate->objspace);
	{
	    OBJSPACE_LOCK_ENTER(closing_gate->objspace);
	    {
		perform_absorption(receiving_gate, closing_gate);
	    }
	    OBJSPACE_LOCK_LEAVE(closing_gate->objspace);
	}
	OBJSPACE_LOCK_LEAVE(receiving_gate->objspace);

	lock_ractor_set();

	ccan_list_del(&closing_gate->gate_node);
	closing_gate->objspace_closed = true;

	unlock_ractor_set();

	if (rb_ractor_status_p(closing_ractor, ractor_terminated)) {
	    rb_remove_from_contained_ractor_tbl(closing_ractor);
	    rb_objspace_free(closing_gate->objspace);
	    closing_ractor->local_objspace = NULL;
	    closing_ractor->local_gate = NULL;
	}
	rb_borrowing_sync_unlock(receiving_ractor);

	receiving_gate->currently_absorbing = false;

	closing_ractor->objspace_absorbed = true;
	objspace_absorb_leave(coordinator);
    }
    RB_VM_UNLOCK();

    rb_native_mutex_lock(&coordinator->next_object_id_lock);
    if (coordinator->prev_id_assigner == closing_ractor) {
	coordinator->prev_id_assigner = receiving_ractor;
    }
    rb_native_mutex_unlock(&coordinator->next_object_id_lock);

    if (already_disabled == Qfalse) rb_objspace_gc_enable(receiving_gate->objspace);
}

static int
mark_absorbed_threads_tbl_i(st_data_t key, st_data_t value, st_data_t data)
{
    struct rb_objspace *objspace = (struct rb_objspace *)data;
    VALUE th = ((rb_thread_t *) key)->self;
    if (objspace == GET_OBJSPACE_OF_VALUE(th)) {
	rb_gc_mark(th);
    }
    return ST_CONTINUE;
}

void
mark_absorbed_threads_tbl(rb_objspace_gate_t *os_gate)
{
    rb_objspace_coordinator_t *coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&coordinator->absorbed_thread_tbl_lock);
    if (coordinator->absorbed_thread_tbl) {
	st_foreach(coordinator->absorbed_thread_tbl, mark_absorbed_threads_tbl_i, (st_data_t)os_gate->objspace);
    }
    rb_native_mutex_unlock(&coordinator->absorbed_thread_tbl_lock);
}

void
rb_add_to_absorbed_threads_tbl(rb_thread_t *th)
{
    rb_objspace_coordinator_t *coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&coordinator->absorbed_thread_tbl_lock);
    st_insert_no_gc(coordinator->absorbed_thread_tbl, (st_data_t)th, INT2FIX(0));
    rb_native_mutex_unlock(&coordinator->absorbed_thread_tbl_lock);
}

void
rb_remove_from_absorbed_threads_tbl(rb_thread_t *th)
{
    rb_objspace_coordinator_t *coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&coordinator->absorbed_thread_tbl_lock);
    st_delete(coordinator->absorbed_thread_tbl, (st_data_t *) &th, NULL);
    rb_native_mutex_unlock(&coordinator->absorbed_thread_tbl_lock);
}


/*
  ------------------------ Borrowing ------------------------
*/

static void
borrowing_alloc_target_push(rb_ractor_t *cr, rb_ractor_t *target)
{
    struct borrowing_target_node_t *btn = ALLOC(struct borrowing_target_node_t);
    btn->target_ractor = target;
    btn->next = cr->borrowing_target_top;
    cr->borrowing_target_top = btn;
}

static rb_ractor_t *
borrowing_alloc_target_pop(rb_ractor_t *cr)
{
    VM_ASSERT(cr->borrowing_target_top != NULL);
    struct borrowing_target_node_t *btn = cr->borrowing_target_top;
    cr->borrowing_target_top = btn->next;
    rb_ractor_t *target = btn->target_ractor;
    free(btn);
    return target;
}

static void
borrowing_count_increment(rb_ractor_t *r)
{
    rb_vm_t *vm = GET_VM();
    rb_native_mutex_lock(&r->borrowing_sync.borrowing_allowed_lock);
    while (!r->borrowing_sync.borrowing_allowed) {
	rb_native_cond_wait(&r->borrowing_sync.borrowing_allowed_cond, &r->borrowing_sync.borrowing_allowed_lock);
    }
    r->borrowing_sync.borrower_count++;
    rb_native_mutex_unlock(&r->borrowing_sync.borrowing_allowed_lock);
}

static void
borrowing_count_decrement(rb_ractor_t *r)
{
    rb_native_mutex_lock(&r->borrowing_sync.borrowing_allowed_lock);
    r->borrowing_sync.borrower_count--;
    VM_ASSERT(r->borrowing_sync.borrower_count >= 0);
    if (r->borrowing_sync.borrower_count == 0) {
	rb_native_cond_signal(&r->borrowing_sync.no_borrowers);
    }
    rb_native_mutex_unlock(&r->borrowing_sync.borrowing_allowed_lock);
}

struct borrowing_data_args {
    rb_ractor_t *borrower;
    rb_ractor_t *target_ractor;
    rb_ractor_t *old_target;
    VALUE (*func)(VALUE);
    VALUE func_args;
    uintptr_t old_borrowing_id;
};

static VALUE
borrowing_enter(VALUE args)
{
    struct borrowing_data_args *borrowing_data = (struct borrowing_data_args *)args;

    rb_ractor_t *borrower = borrowing_data->borrower;
    rb_ractor_t *target_ractor = borrowing_data->target_ractor;

    VM_ASSERT(borrower == GET_RACTOR());

    borrowing_data->old_borrowing_id = borrower->borrowing_sync.borrowing_id;
    borrower->borrowing_sync.borrowing_id = (uintptr_t)borrowing_data;

    if (target_ractor == borrower) {
	target_ractor = NULL;
    }
    else if (!!target_ractor) {
	borrowing_alloc_target_push(borrower, target_ractor);
	borrowing_count_increment(target_ractor);
    }

    rb_objspace_gate_t *borrower_gate = borrower->local_gate;

    borrowing_data->old_target = borrower_gate->alloc_target_ractor;
    borrower_gate->alloc_target_ractor = target_ractor;

    return Qnil;
}

static VALUE
borrowing_exit(VALUE args)
{
    struct borrowing_data_args *borrowing_data = (struct borrowing_data_args *)args;

    rb_ractor_t *borrower = borrowing_data->borrower;
    rb_ractor_t *target_to_restore = borrowing_data->old_target;

    VM_ASSERT(borrower == GET_RACTOR());
    VM_ASSERT(target_to_restore != borrower);

    rb_objspace_gate_t *borrower_gate = borrower->local_gate;

    rb_ractor_t *finished_target = borrower_gate->alloc_target_ractor;
    VM_ASSERT(finished_target != borrower);
    uintptr_t finished_borrowing_id = borrower->borrowing_sync.borrowing_id;

    borrower_gate->alloc_target_ractor = target_to_restore;
    borrower->borrowing_sync.borrowing_id = (uintptr_t)borrowing_data->old_borrowing_id;

    if (!!finished_target) {
	rb_borrowing_sync_lock(finished_target);
	if (LIKELY(!finished_target->borrowing_sync.borrowing_closed)) {
	    remove_received_obj_list(finished_target->local_gate, finished_borrowing_id);
	}
	rb_borrowing_sync_unlock(finished_target);
	borrowing_count_decrement(finished_target);
#if VM_CHECK_MODE > 0
	rb_ractor_t *popped_target = borrowing_alloc_target_pop(borrower);
	VM_ASSERT(popped_target == finished_target);
#else
	borrowing_alloc_target_pop(borrower);
#endif
    }

    return Qnil;
}

static VALUE
run_redirected_func(VALUE args)
{
    struct borrowing_data_args *borrowing_data = (struct borrowing_data_args *)args;
    VALUE result = borrowing_data->func(borrowing_data->func_args);
    rb_objspace_gate_t *borrower_gate = borrowing_data->borrower->local_gate;
    if (!SPECIAL_CONST_P(result) && GET_OBJSPACE_OF_VALUE(result) != borrower_gate->objspace) {
	VM_ASSERT(FL_TEST(result, FL_SHAREABLE));
	rb_register_new_external_reference(borrower_gate, result);
    }
    return result;
}

VALUE
rb_attempt_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args, bool *borrowing_success)
{
    struct borrowing_data_args borrowing_data = {
	.borrower = GET_RACTOR(),
	.target_ractor = target_ractor,
	.old_target = NULL,
	.func = func,
	.func_args = func_args,
	.old_borrowing_id = 0,
    };
    VALUE bd_args = (VALUE)&borrowing_data;
    borrowing_enter(bd_args);
    if (UNLIKELY(target_ractor && target_ractor->borrowing_sync.borrowing_closed)) {
	if (borrowing_success) *borrowing_success = false;
	borrowing_exit(bd_args);
	return Qfalse;
    }
    else {
	if (borrowing_success) *borrowing_success = true;
	return rb_ensure(run_redirected_func, bd_args, borrowing_exit, bd_args);
    }
}

VALUE
rb_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args)
{
    bool success;
    VALUE ret = rb_attempt_run_with_redirected_allocation(target_ractor, func, func_args, &success);
    if (LIKELY(success)) {
	return ret;
    }
    else {
	rb_bug("Failed to borrow from Ractor #%d", target_ractor->pub.id);
    }
}

rb_ractor_t *
rb_current_allocating_ractor(void)
{
    rb_ractor_t *r = ruby_single_main_ractor ? ruby_single_main_ractor : GET_RACTOR();
    if (r->local_gate && r->local_gate->alloc_target_ractor) {
	return r->local_gate->alloc_target_ractor;
    }
    return r;
}

bool
rb_redirecting_allocation(void)
{
    rb_ractor_t *r = ruby_single_main_ractor ? ruby_single_main_ractor : GET_RACTOR();
    if (r->local_gate && r->local_gate->alloc_target_ractor) {
	return true;
    }
    return false;
}

void
lock_own_borrowable_page(rb_ractor_t *cr, struct rb_borrowing_location_lock *location_lock)
{
    if (location_lock->page_lock_lev == 0) {
	rb_native_mutex_lock(&location_lock->page_lock);
	location_lock->page_lock_owner = cr;
    }
    location_lock->page_lock_lev++;
    location_lock->page_recently_locked = true;
}

void
unlock_own_borrowable_page(rb_ractor_t *cr, struct rb_borrowing_location_lock *location_lock)
{
    location_lock->page_lock_lev--;
    if (location_lock->page_lock_lev == 0) {
	location_lock->page_lock_owner = NULL;
	rb_native_mutex_unlock(&location_lock->page_lock);
    }
}

void
borrowing_location_lock_init(struct rb_borrowing_location_lock *location_lock)
{
    rb_native_mutex_initialize(&location_lock->page_lock);
    location_lock->page_lock_owner = NULL;
    location_lock->page_lock_lev = 0;
    location_lock->page_recently_locked = false;
}

void
borrowing_location_lock_release(struct rb_borrowing_location_lock *location_lock)
{
    rb_native_mutex_destroy(&location_lock->page_lock);
}


/*
  ------------------------ GC Coordination ------------------------
*/

void
rb_gc_safe_lock_enter(rb_gc_safe_lock_t *gs_lock)
{
    struct rb_ractor_struct *cr = GET_RACTOR();
    if (gs_lock->lock_owner != cr) {
	if (!rb_during_gc()) gs_lock->gc_previously_disabled = rb_gc_disable();
	rb_native_mutex_lock(&gs_lock->lock);
	gs_lock->lock_owner = cr;
    }
    gs_lock->lock_lev++;
}

void
rb_gc_safe_lock_leave(rb_gc_safe_lock_t *gs_lock)
{
    gs_lock->lock_lev--;
    if (gs_lock->lock_lev == 0) {
	gs_lock->lock_owner = NULL;
	rb_native_mutex_unlock(&gs_lock->lock);
	if ((!rb_during_gc()) && gs_lock->gc_previously_disabled == Qfalse) rb_gc_enable();
    }
}

bool
rb_gc_safe_lock_acquired(rb_gc_safe_lock_t *gs_lock)
{
    return gs_lock->lock_owner == GET_RACTOR();
}

void
rb_gc_safe_lock_initialize(rb_gc_safe_lock_t *gs_lock)
{
    rb_nativethread_lock_initialize(&gs_lock->lock);
    gs_lock->lock_owner = NULL;
    gs_lock->lock_lev = 0;
    gs_lock->gc_previously_disabled = Qfalse;
}

void
rb_gc_safe_lock_destroy(rb_gc_safe_lock_t *gs_lock)
{
    rb_nativethread_lock_destroy(&gs_lock->lock);
}

void
objspace_lock_enter(rb_objspace_gate_t *os_gate, rb_ractor_t *cr)
{
    if (os_gate->objspace_lock_owner != cr) {
	rb_native_mutex_lock(&os_gate->objspace_lock);
	os_gate->objspace_lock_owner = cr;
    }
    os_gate->objspace_lock_level++;
}

void
objspace_lock_leave(rb_objspace_gate_t *os_gate, rb_ractor_t *cr)
{
    VM_ASSERT(os_gate->objspace_lock_owner == cr);
    os_gate->objspace_lock_level--;
    if (os_gate->objspace_lock_level == 0) {
	os_gate->objspace_lock_owner = NULL;
	rb_native_mutex_unlock(&os_gate->objspace_lock);
    }
}

bool
objspace_locked(rb_objspace_gate_t *os_gate)
{
    return os_gate->objspace_lock_owner == GET_RACTOR();
}

void
local_gc_running_on(rb_objspace_gate_t *local_gate)
{
    VM_ASSERT(local_gate->objspace == GET_RACTOR()->local_gate->gc_target);
    rb_native_mutex_lock(&local_gate->running_local_gc_lock);
    local_gate->running_local_gc = true;
    rb_native_mutex_unlock(&local_gate->running_local_gc_lock);
}

void
local_gc_running_off(rb_objspace_gate_t *local_gate)
{
    VM_ASSERT(local_gate->objspace == GET_RACTOR()->local_gate->gc_target);
    rb_native_mutex_lock(&local_gate->running_local_gc_lock);
    local_gate->running_local_gc = false;
    rb_native_mutex_unlock(&local_gate->running_local_gc_lock);
    rb_native_cond_broadcast(&local_gate->local_gc_stop_cond);
}

void
begin_local_gc_section(rb_objspace_gate_t *local_gate, rb_ractor_t *cr)
{
    if (!local_gate->running_global_gc && local_gate->local_gc_level == 0) {
	rb_ractor_borrowing_barrier_begin(local_gate->ractor);
    }

    local_gate->local_gc_level++;

    local_gc_running_on(local_gate);
}

void
end_local_gc_section(rb_objspace_gate_t *local_gate, rb_ractor_t *cr)
{
    local_gc_running_off(local_gate);

    local_gate->local_gc_level--;

    if (!local_gate->running_global_gc && local_gate->local_gc_level == 0) {
	rb_ractor_borrowing_barrier_end(local_gate->ractor);
    }
}

void
begin_global_gc_section(rb_objspace_coordinator_t *coordinator, rb_objspace_gate_t *local_gate, unsigned int *lev)
{
    rb_ractor_t *cr = GET_RACTOR();
    RB_VM_LOCK_ENTER_LEV(lev);
    
    VM_COND_AND_BARRIER_WAIT(GET_VM(), coordinator->global_gc_finished, !coordinator->global_gc_underway);
    local_gate->running_global_gc = true;
    coordinator->global_gc_underway = true;
    rb_vm_barrier();
}

void
end_global_gc_section(rb_objspace_coordinator_t *coordinator, rb_objspace_gate_t *local_gate, unsigned int *lev)
{
    local_gate->running_global_gc = false;
    coordinator->global_gc_underway = false;
    rb_native_cond_broadcast(&coordinator->global_gc_finished);
    RB_VM_LOCK_LEAVE_LEV(lev);
}

bool
gc_deactivated(rb_objspace_coordinator_t *coordinator)
{
    return coordinator->gc_deactivated;
}

VALUE
gc_deactivate_no_rest(rb_objspace_coordinator_t *coordinator)
{
    int old = coordinator->gc_deactivated;
    coordinator->gc_deactivated = true;
    return RBOOL(old);
}

VALUE
rb_gc_deactivate(rb_objspace_coordinator_t *coordinator)
{
    rb_vm_t *vm = GET_VM();
    VALUE old;
    rb_gc_deactivate_prepare(vm->objspace);
    old = gc_deactivate_no_rest(coordinator);
    return old;
}

static bool
local_limits_in_use(rb_objspace_gate_t *os_gate)
{
    return os_gate->objspace != ruby_single_main_objspace && rb_during_local_gc();
}

#if VM_CHECK_MODE > 0
bool
rb_ractor_safe_gc_state(void)
{
    if (!rb_multi_ractor_p()) {
	return true;
    }
    rb_vm_t *vm = GET_VM();
    rb_ractor_t *cr = GET_RACTOR();
    if (vm->ractor.sync.lock_owner == cr) {
	return true;
    }
    return !rb_during_global_gc();
}
#endif

bool
global_gc_needed(void)
{
    bool needed;
    rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&objspace_coordinator->rglobalgc.shared_tracking_lock);
    needed = objspace_coordinator->rglobalgc.need_global_gc;
    rb_native_mutex_unlock(&objspace_coordinator->rglobalgc.shared_tracking_lock);
    return needed;
}

void
arrange_next_gc_global_status(double sharedobject_limit_factor)
{
    rb_objspace_coordinator_t *objspace_coordinator = rb_get_objspace_coordinator();
    rb_native_mutex_lock(&objspace_coordinator->rglobalgc.shared_tracking_lock);

    int shared_plus_local_immune = objspace_coordinator->rglobalgc.shared_objects_total + local_immune_objects_global_count();

    if (rb_during_global_gc()) {
	objspace_coordinator->rglobalgc.global_gc_threshold = (size_t)(shared_plus_local_immune * sharedobject_limit_factor);
    }
    else if (rb_multi_ractor_p() && shared_plus_local_immune > objspace_coordinator->rglobalgc.global_gc_threshold) {
	objspace_coordinator->rglobalgc.need_global_gc = true;
    }

    rb_native_mutex_unlock(&objspace_coordinator->rglobalgc.shared_tracking_lock);
}

bool
mark_externally_modifiable_tables(rb_objspace_gate_t *os_gate)
{
    if (!local_limits_in_use(os_gate)) return true;

    rb_objspace_gate_t *local_gate = os_gate;
    if (!shared_references_all_marked(os_gate)) {
	mark_shared_reference_tbl(os_gate);
	return false;
    }

    return true;
}

void
global_gc_for_each_objspace(rb_vm_t *vm, rb_objspace_gate_t *runner_gate, void (gc_func)(struct rb_objspace *objspace))
{
    rb_objspace_gate_t *local_gate = NULL;
    struct rb_objspace *prev_target = runner_gate->gc_target;
    ccan_list_for_each(&vm->objspace_set, local_gate, gate_node) {
	struct rb_objspace *os = local_gate->objspace;
	runner_gate->gc_target = os;
	gc_func(os);
    }
    runner_gate->gc_target = prev_target;
}

void
rb_objspace_call_finalizer_for_each_ractor(rb_vm_t *vm)
{
    rb_ractor_t *r = NULL;
    ccan_list_for_each(&vm->ractor.set, r, vmlr_node) {
	if (r != vm->ractor.main_ractor) {
	    rb_objspace_call_finalizer(r->local_objspace);
	}
    }
    rb_objspace_call_finalizer(vm->objspace);
}

static void
gc_writebarrier_parallel_objspace(VALUE a, VALUE b, rb_objspace_gate_t *os_gate)
{
    rb_native_mutex_lock(&os_gate->running_local_gc_lock);
    while (os_gate->running_local_gc) {
	rb_native_cond_wait(&os_gate->local_gc_stop_cond, &os_gate->running_local_gc_lock);
    }
    rb_gc_writebarrier_gc_blocked(os_gate->objspace, a, b);
    rb_native_mutex_unlock(&os_gate->running_local_gc_lock);
}

void
rb_gc_writebarrier_multi_objspace(VALUE a, VALUE b, struct rb_objspace *current_objspace)
{
    VM_ASSERT(!SPECIAL_CONST_P(a));
    VM_ASSERT(!SPECIAL_CONST_P(b));

    WITH_OBJSPACE_GATE_ENTER(b, b_gate);
    {
	struct rb_objspace *b_objspace = b_gate->objspace;
	if (FL_TEST_RAW(b, FL_SHAREABLE)) {
	    if (UNLIKELY(GET_OBJSPACE_OF_VALUE(a) != b_objspace)) {
		WITH_OBJSPACE_GATE_ENTER(a, a_gate);
		{
		    if (LIKELY(a_gate != b_gate)) register_new_external_reference(a_gate, b_gate, b);
		}
		WITH_OBJSPACE_GATE_LEAVE(a_gate);
	    }

	    if (LIKELY(b_objspace == current_objspace || b_objspace == rb_current_allocating_ractor()->local_objspace)) {
		rb_gc_writebarrier_gc_blocked(b_objspace, a, b);
	    }
	    else {
		gc_writebarrier_parallel_objspace(a, b, b_gate);
	    }
	}
	else {
	    VM_ASSERT(b_objspace == current_objspace || b_objspace == rb_current_allocating_ractor()->local_objspace);
	    VM_ASSERT(GET_OBJSPACE_OF_VALUE(a) == b_objspace || GET_RACTOR()->during_ractor_copy_or_move);

	    rb_gc_writebarrier_gc_blocked(b_objspace, a, b);
	}
    }
    WITH_OBJSPACE_GATE_LEAVE(b_gate);
}
