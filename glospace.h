#ifndef RUBY_GLOSPACE_H
#define RUBY_GLOSPACE_H

#include "internal/gc.h"
#include "internal/numeric.h"
#include "ractor_core.h"
#include "vm_core.h"
#include "vm_sync.h"
#include "vm_callinfo.h"

typedef struct rb_objspace rb_objspace_t;
typedef struct rb_heap_struct rb_heap_t;

typedef struct rb_global_space {
    unsigned long long next_object_id;

    rb_ractor_t *prev_id_assigner;
    rb_nativethread_lock_t next_object_id_lock;

    st_table *absorbed_thread_tbl;
    rb_nativethread_lock_t absorbed_thread_tbl_lock;

    struct {
        bool need_global_gc;
	size_t shared_objects_limit;
	size_t shared_objects_total;
	rb_nativethread_lock_t shared_tracking_lock;
    } rglobalgc; //TODO: Call global GC upon hitting shared object limit

    struct {
	struct rb_ractor_chain ractor_chain;
	rb_nativethread_cond_t removal_cond;
    } ogs_tracking;

    struct {
	rb_nativethread_lock_t mode_lock;
	rb_nativethread_cond_t mode_change_cond;
	int objspace_readers;
	int absorbers;
    } absorption;

    struct {
	rb_nativethread_lock_t global_pages_lock;

        struct heap_page **sorted;
        size_t allocated_pages;
        size_t sorted_length;
        uintptr_t range[2];
    } all_pages;
} rb_global_space_t;

#define all_pages_sorted_global       global_space->all_pages.sorted
#define all_allocated_pages_global    global_space->all_pages.allocated_pages
#define all_pages_sorted_length_global global_space->all_pages.sorted_length
#define all_pages_lomem_global	global_space->all_pages.range[0]
#define all_pages_himem_global	global_space->all_pages.range[1]

struct objspace_local_stats {
    unsigned int field_count;
    size_t **stats;
};

struct objspace_local_data {
    rb_objspace_t *objspace;
    rb_ractor_t *ractor;

    st_table *finalizer_table;

    st_table *contained_ractor_tbl;
    rb_nativethread_lock_t contained_ractor_tbl_lock;

    st_table *wmap_referenced_obj_tbl;
    rb_nativethread_lock_t wmap_referenced_obj_tbl_lock;

    st_table *id_to_obj_tbl;
    st_table *obj_to_id_tbl;
    rb_nativethread_lock_t obj_id_lock;

    st_table *shared_reference_tbl;
    rb_nativethread_lock_t shared_reference_tbl_lock;
    st_table *external_reference_tbl;
    rb_nativethread_lock_t external_reference_tbl_lock;

    struct ccan_list_head zombie_threads;
    rb_nativethread_lock_t zombie_threads_lock;

    struct ccan_list_node objspace_node;
    bool objspace_closed;

    struct objspace_local_stats gc_stats;
    struct objspace_local_stats size_pool_stats[SIZE_POOL_COUNT];

    bool currently_absorbing;

    bool external_writebarrier_allowed;
    rb_nativethread_lock_t external_writebarrier_allowed_lock;
    rb_nativethread_cond_t external_writebarrier_allowed_cond;

    bool running_global_gc;
    bool waiting_for_object_graph_safety;

    int local_gc_level;

    const struct rb_callcache *global_cc_cache_table[VM_GLOBAL_CC_CACHE_TABLE_SIZE]; // vm_eval.c

    rb_nativethread_lock_t heap_lock;
    rb_ractor_t *heap_lock_owner;
    int heap_lock_level;

    st_table *received_obj_tbl;
    rb_nativethread_lock_t received_obj_tbl_lock;

    rb_ractor_t *alloc_target_ractor;

    struct rb_objspace *global_gc_current_target;

    bool belong_to_single_main_ractor;

    struct rb_objspace *current_parent_objspace;

    bool freeing_all;
};

typedef struct gc_reference_status {
    rb_atomic_t *refcount;
    int status;
} gc_reference_status_t;

enum {
    shared_object_local,
    shared_object_unmarked,
    shared_object_marked,
    shared_object_discovered_and_marked,
    shared_object_added_externally,
};

rb_global_space_t *rb_global_space_init(void);
void rb_global_space_free(rb_global_space_t *global_space);
rb_global_space_t *get_global_space(void);

#define rb_global_space (*get_global_space())

rb_heap_t;
void rb_ractor_chains_register(rb_ractor_t *r);
void rb_ractor_object_graph_safety_advance(rb_ractor_t *r, unsigned int reason);

void rb_ractor_object_graph_safety_withdraw(rb_ractor_t *r, unsigned int reason);
int st_insert_no_gc(st_table *tab, st_data_t key, st_data_t value);
void mark_global_cc_cache_table(rb_objspace_t *objspace);
void rb_register_new_external_reference(rb_objspace_t *receiving_objspace, VALUE obj);

size_t total_pages_in_heap(rb_heap_t *heap);
rb_heap_t *select_heap(rb_objspace_t *objspace, int size_pool_idx, bool eden);
void size_pool_allocatable_pages_update(rb_objspace_t *objspace, size_t additional_pages[SIZE_POOL_COUNT]);
struct heap_page *get_freepages(rb_heap_t *heap);
void heap_append_free_page(rb_heap_t *heap, struct heap_page *page);
void absorb_page_into_objspace(rb_objspace_t *objspace, struct heap_page *page, int size_pool_idx, bool eden);
struct heap_page *heap_get_top_page(rb_heap_t *heap);

#ifdef RUBY_THREAD_PTHREAD_H
# define BARRIER_WAITING(vm) (vm->ractor.sched.barrier_waiting)
#else
# define BARRIER_WAITING(vm) (vm->ractor.sync.barrier_waiting)
#endif

static void
barrier_join_within_vm_lock(rb_vm_t *vm, rb_ractor_t *cr)
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

#define VM_COND_AND_BARRIER_WAIT(vm, cond, state_to_check) do { \
    ASSERT_vm_locking(); \
    rb_ractor_t *_cr = GET_RACTOR(); \
    rb_ractor_object_graph_safety_advance(_cr, OGS_FLAG_COND_AND_BARRIER); \
    while (BARRIER_WAITING(vm)) { \
	barrier_join_within_vm_lock(vm, _cr); \
    } \
    bool _need_repeat = true; \
    if(state_to_check) { \
	_need_repeat = false; \
    } \
    else { \
	rb_vm_cond_wait(vm, &cond); \
	_need_repeat = !state_to_check || BARRIER_WAITING(vm); \
    } \
    rb_ractor_object_graph_safety_withdraw(_cr, OGS_FLAG_COND_AND_BARRIER); \
    if (!_need_repeat) break; \
} while(true)

#define COND_AND_BARRIER_WAIT(vm, lock, cond, state_to_check) do { \
    ASSERT_vm_locking(); \
    rb_ractor_t *_cr = GET_RACTOR(); \
    rb_ractor_object_graph_safety_advance(_cr, OGS_FLAG_COND_AND_BARRIER); \
    while (BARRIER_WAITING(vm)) { \
	rb_native_mutex_unlock(&lock); \
	barrier_join_within_vm_lock(vm, _cr); \
	rb_native_mutex_lock(&lock); \
    } \
    bool _need_repeat = true; \
    if(state_to_check) { \
	_need_repeat = false; \
    } \
    else { \
	rb_native_cond_wait(&cond, &lock); \
	_need_repeat = !state_to_check || BARRIER_WAITING(vm); \
    } \
    rb_ractor_object_graph_safety_withdraw(_cr, OGS_FLAG_COND_AND_BARRIER); \
    if (!_need_repeat) break; \
} while(true)

static void objspace_read_enter(rb_global_space_t *global_space);
static void objspace_read_leave(rb_global_space_t *global_space);

void rb_absorb_objspace_of_closing_ractor(rb_ractor_t *receiving_ractor, rb_ractor_t *closing_ractor);

void rb_add_to_absorbed_threads_tbl(rb_thread_t *th);
void rb_remove_from_absorbed_threads_tbl(rb_thread_t *th);

void rb_add_zombie_thread(rb_thread_t *th);
struct objspace_local_data *objspace_get_local_data(rb_objspace_t *objspace);

void rb_add_to_contained_ractor_tbl(rb_ractor_t *r);
void rb_remove_from_contained_ractor_tbl(rb_ractor_t *r);

void rb_register_new_external_wmap_reference(VALUE *ptr);
void rb_remove_from_external_weak_tables(VALUE *ptr);

static gc_reference_status_t *
get_reference_status(st_table *tbl, VALUE obj)
{
    st_data_t data;
    bool found = !!st_lookup(tbl, (st_data_t)obj, &data);
    if (found) {
	return (gc_reference_status_t *)data;
    }
    else {
	return NULL;
    }
}

static void
set_reference_status(st_table *tbl, VALUE obj, gc_reference_status_t *rs)
{
    st_insert_no_gc(tbl, (st_data_t)obj, (st_data_t)rs);
}

static void
delete_reference_status(st_table *tbl, VALUE obj)
{
    st_data_t rs;
    st_delete(tbl, (st_data_t *)&obj, &rs);
    free(rs);
}

VALUE rb_attempt_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args, bool *borrowing_success);
bool rb_redirecting_allocation(void);
void rb_borrowing_status_pause(rb_ractor_t *cr);
void rb_borrowing_status_resume(rb_ractor_t *cr);
rb_ractor_t * rb_current_allocating_ractor(void);

static void
add_external_reference_usage(rb_objspace_t *objspace, VALUE obj, gc_reference_status_t *rs)
{
    WITH_OBJSPACE_LOCAL_DATA_ENTER(obj, source_data);
    {
	rb_native_mutex_lock(&source_data->shared_reference_tbl_lock);
	gc_reference_status_t *local_rs = get_reference_status(source_data->shared_reference_tbl, obj);

	if (local_rs) {
	    ATOMIC_INC(*local_rs->refcount);
	}
	else {
	    VM_ASSERT(rb_during_gc() || (rb_current_allocating_ractor() == source_data->ractor) || (rb_current_allocating_ractor() == objspace_get_local_data(objspace)->ractor));

	    rb_atomic_t *refcount = malloc(sizeof(rb_atomic_t));
	    *refcount = 1;

	    local_rs = malloc(sizeof(gc_reference_status_t));
	    local_rs->refcount = refcount;
	    local_rs->status = shared_object_local;
	    set_reference_status(source_data->shared_reference_tbl, obj, local_rs);

	    rb_global_space_t *global_space = &rb_global_space;
	    rb_native_mutex_lock(&global_space->rglobalgc.shared_tracking_lock);
	    global_space->rglobalgc.shared_objects_total++;
	    rb_native_mutex_unlock(&global_space->rglobalgc.shared_tracking_lock);
	}
	rb_native_mutex_unlock(&source_data->shared_reference_tbl_lock);
	rs->refcount = local_rs->refcount;
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(source_data);
}

static void
register_new_external_reference(rb_objspace_t *receiving_objspace, rb_objspace_t *source_objspace, VALUE obj)
{
    VM_ASSERT(!RB_SPECIAL_CONST_P(obj));
    VM_ASSERT(GET_OBJSPACE_OF_VALUE(obj) == source_objspace);
    VM_ASSERT(receiving_objspace != source_objspace);

    struct objspace_local_data *receiving_data = objspace_get_local_data(receiving_objspace);

    rb_native_mutex_lock(&receiving_data->external_reference_tbl_lock);

    gc_reference_status_t *rs = get_reference_status(receiving_data->external_reference_tbl, obj);
    bool new_addition = !rs;

    if (new_addition) {
	gc_reference_status_t *rs = malloc(sizeof(gc_reference_status_t));

	add_external_reference_usage(receiving_objspace, obj, rs);

	if (receiving_objspace == rb_current_allocating_ractor()->local_objspace) {
	    rs->status = shared_object_unmarked;
	}
	else {
	    rs->status = shared_object_added_externally;
	}
	set_reference_status(receiving_data->external_reference_tbl, obj, rs);
    }

    rb_native_mutex_unlock(&receiving_data->external_reference_tbl_lock);
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

static void
confirm_externally_added_external_references(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    st_foreach(local_data->external_reference_tbl, confirm_externally_added_external_references_i, NULL);
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
}

static void
drop_external_reference_usage(rb_objspace_t *objspace, VALUE obj, gc_reference_status_t *rs)
{
    rb_atomic_t *refcount = rs->refcount;
    rb_atomic_t prev_count = RUBY_ATOMIC_FETCH_SUB(*refcount, 1);
    if (prev_count == 1) {
	WITH_OBJSPACE_LOCAL_DATA_ENTER(obj, source_data);
	{
	    rb_native_mutex_lock(&source_data->shared_reference_tbl_lock);
	    if (*refcount == 0) {
		delete_reference_status(source_data->shared_reference_tbl, obj);
		free(refcount);

		rb_global_space_t *global_space = &rb_global_space;
		rb_native_mutex_lock(&global_space->rglobalgc.shared_tracking_lock);
		global_space->rglobalgc.shared_objects_total--;
		rb_native_mutex_unlock(&global_space->rglobalgc.shared_tracking_lock);
	    }
	    rb_native_mutex_unlock(&source_data->shared_reference_tbl_lock);
	}
	WITH_OBJSPACE_LOCAL_DATA_LEAVE(source_data);
    }
    free(rs);
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
external_references_none_marked(rb_objspace_t *objspace)
{
    bool none_marked = true;
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    st_foreach(local_data->external_reference_tbl, external_references_none_marked_i, (st_data_t)&none_marked);
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
    return none_marked;
}

static void
mark_in_external_reference_tbl(rb_objspace_t *objspace, VALUE obj)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    gc_reference_status_t *rs = get_reference_status(local_data->external_reference_tbl, obj);
    if (rs) {
	if (rs->status == shared_object_unmarked) {
	    rs->status = shared_object_marked;
	}
    }
    else {
	rs = malloc(sizeof(gc_reference_status_t));
	rs->refcount = NULL;
	rs->status = shared_object_discovered_and_marked;
	set_reference_status(local_data->external_reference_tbl, obj, rs);
    }
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
}

bool rb_gc_object_marked(VALUE obj);
bool rb_gc_object_local_immune(VALUE obj);
void gc_give_local_immunity_no_check(VALUE obj);

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
shared_references_all_marked(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    bool all_marked = true;

    rb_native_mutex_lock(&local_data->shared_reference_tbl_lock);
    st_foreach(local_data->shared_reference_tbl, shared_references_all_marked_i, (st_data_t)&all_marked);
    rb_native_mutex_unlock(&local_data->shared_reference_tbl_lock);
    return all_marked;
}

static void
mark_shared_reference_tbl(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->shared_reference_tbl_lock);
    rb_mark_set(local_data->shared_reference_tbl);
    rb_native_mutex_unlock(&local_data->shared_reference_tbl_lock);
}

static void
mark_contained_ractor_tbl(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->contained_ractor_tbl_lock);
    rb_mark_set(local_data->contained_ractor_tbl);
    rb_native_mutex_unlock(&local_data->contained_ractor_tbl_lock);
}

static int
confirm_discovered_external_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    rb_objspace_t *objspace = (rb_objspace_t *)argp;
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    VALUE obj = (VALUE)key;
    if (rs->status == shared_object_discovered_and_marked) {
	add_external_reference_usage(objspace, obj, rs);
	rs->status = shared_object_marked;
    }
    return ST_CONTINUE;
}

static void
confirm_discovered_external_references(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    st_foreach(local_data->external_reference_tbl, confirm_discovered_external_references_i, (st_data_t)objspace);
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
}

static bool
local_limits_in_use(rb_objspace_t *objspace)
{
    return objspace != ruby_single_main_objspace && rb_during_local_gc();
}

static int
keep_marked_shared_object_references_i(st_data_t key, st_data_t value, st_data_t argp, int error)
{
    rb_objspace_t *objspace = (rb_objspace_t *)argp;
    gc_reference_status_t *rs = (gc_reference_status_t *)value;
    VALUE obj = (VALUE)key;
    switch (rs->status) {
	case shared_object_unmarked:
	    drop_external_reference_usage(objspace, obj, rs);
	    return ST_DELETE;
	case shared_object_marked:
	    VM_ASSERT(local_limits_in_use(objspace) || rb_gc_object_marked(obj));
	    rs->status = shared_object_unmarked;
	    return ST_CONTINUE;
	case shared_object_added_externally:
	    if (LIKELY(local_limits_in_use(objspace) || rb_gc_object_marked(obj))) {
		rs->status = shared_object_unmarked;
		return ST_CONTINUE;
	    }
	    else {
		drop_external_reference_usage(objspace, obj, rs);
		return ST_DELETE;
	    }
	default:
	    rb_bug("update_shared_object_references_i: unreachable");
    }
}

static void
keep_marked_shared_object_references(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    st_foreach(local_data->external_reference_tbl, keep_marked_shared_object_references_i, (st_data_t)objspace);
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
}

static bool
object_graph_safety_p(rb_global_space_t *global_space, rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    struct rb_ractor_chain_node *ogs_node = local_data->ractor->ogs_chain_node->next_node;
    while (ogs_node) {
	if (!RUBY_ATOMIC_LOAD(ogs_node->value)) {
	    return false;
	}
	ogs_node = ogs_node->next_node;
    }
    return true;
}

static void
wait_for_object_graph_safety(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    if (local_data->running_global_gc) return;

    rb_global_space_t *global_space = &rb_global_space;

    local_data->waiting_for_object_graph_safety = true;

    rb_native_mutex_lock(&local_data->external_writebarrier_allowed_lock);
    local_data->external_writebarrier_allowed = true;
    rb_native_mutex_unlock(&local_data->external_writebarrier_allowed_lock);
    rb_native_cond_broadcast(&local_data->external_writebarrier_allowed_cond);

    rb_ractor_borrowing_barrier_end(local_data->ractor);

    rb_native_mutex_lock(&global_space->ogs_tracking.ractor_chain.lock);
    while (!object_graph_safety_p(global_space, objspace)) {
	rb_native_cond_wait(&global_space->ogs_tracking.removal_cond, &global_space->ogs_tracking.ractor_chain.lock);
    }
    rb_native_mutex_unlock(&global_space->ogs_tracking.ractor_chain.lock);

    rb_ractor_borrowing_barrier_begin(local_data->ractor);

    rb_native_mutex_lock(&local_data->external_writebarrier_allowed_lock);
    local_data->external_writebarrier_allowed = false;
    rb_native_mutex_unlock(&local_data->external_writebarrier_allowed_lock);

    local_data->waiting_for_object_graph_safety = false;
}

static bool
mark_externally_modifiable_tables(rb_objspace_t *objspace)
{
    if (!local_limits_in_use(objspace)) return true;

    wait_for_object_graph_safety(objspace);

    bool new_marks_added = false;

    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    if (!shared_references_all_marked(objspace)) {
	mark_shared_reference_tbl(objspace);
	new_marks_added = true;
    }

    return !new_marks_added;
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

static void
gc_update_external_weak_references(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->wmap_referenced_obj_tbl_lock);
    st_foreach(local_data->wmap_referenced_obj_tbl, update_external_weak_references_i, (st_data_t)objspace);
    rb_native_mutex_unlock(&local_data->wmap_referenced_obj_tbl_lock);
}

static void
update_shared_object_references(rb_objspace_t *objspace)
{
    confirm_discovered_external_references(objspace);
    keep_marked_shared_object_references(objspace);
    VM_ASSERT(external_references_none_marked(objspace));
}

static void
objspace_read_enter(rb_global_space_t *global_space)
{
    rb_objspace_t *current_objspace = GET_OBJSPACE();
    struct objspace_local_data *local_data = objspace_get_local_data(current_objspace);
    if (current_objspace && local_data->currently_absorbing) return;
    
    rb_native_mutex_lock(&global_space->absorption.mode_lock);

    VM_ASSERT(global_space->absorption.objspace_readers == 0 || global_space->absorption.absorbers == 0);

    while (global_space->absorption.absorbers > 0) {
	rb_native_cond_wait(&global_space->absorption.mode_change_cond, &global_space->absorption.mode_lock);
    }
    global_space->absorption.objspace_readers++;
    rb_native_mutex_unlock(&global_space->absorption.mode_lock);
}

static void
objspace_read_leave(rb_global_space_t *global_space)
{
    rb_objspace_t *current_objspace = GET_OBJSPACE();
    struct objspace_local_data *local_data = objspace_get_local_data(current_objspace);
    if (current_objspace && local_data->currently_absorbing) return;
    
    rb_native_mutex_lock(&global_space->absorption.mode_lock);

    VM_ASSERT(global_space->absorption.objspace_readers == 0 || global_space->absorption.absorbers == 0);

    global_space->absorption.objspace_readers--;
    if (global_space->absorption.objspace_readers == 0) {
	rb_native_cond_broadcast(&global_space->absorption.mode_change_cond);
    }
    rb_native_mutex_unlock(&global_space->absorption.mode_lock);
}

void begin_local_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, rb_ractor_t *cr);
void end_local_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, rb_ractor_t *cr);
void begin_global_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, unsigned int *lev);
void end_global_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, unsigned int *lev);

#if VM_CHECK_MODE > 0
static bool
check_local_immune_chldren_i(VALUE obj, void *ptr)
{
    if (!SPECIAL_CONST_P(obj) && !rb_gc_object_local_immune(obj)) {
	rp(obj);
	rb_bug("child object is not local-immune");
    }
}

static void
check_local_immune_children(VALUE obj)
{
    if (!SPECIAL_CONST_P(obj)) rb_objspace_reachable_objects_from(obj, check_local_immune_chldren_i, NULL);
}
#endif

static void
rb_gc_give_local_immunity(VALUE obj)
{
    gc_give_local_immunity_no_check(obj);
#if VM_CHECK_MODE > 0
    check_local_immune_children(obj);
#endif
}

#define MUTABLE_SHAREABLE(obj) (!OBJ_FROZEN(obj) && FL_TEST_RAW(obj, FL_SHAREABLE))
#define NEEDS_LOCAL_IMMUNE_CHILDREN(obj) (MUTABLE_SHAREABLE(obj) || RVALUE_LOCAL_IMMUNE(obj))

static void
rb_gc_give_local_immunity_traversal_i(VALUE obj, void *ptr)
{
    if (!SPECIAL_CONST_P(obj) && !rb_gc_object_local_immune(obj)) {
	gc_give_local_immunity_no_check(obj);

	if (!MUTABLE_SHAREABLE(obj)) {
	    rb_objspace_reachable_objects_from(obj, rb_gc_give_local_immunity_traversal_i, NULL);
	}
    }

    rp(obj);
#if VM_CHECK_MODE > 0
    check_local_immune_children(obj);
#endif

}

static void
rb_gc_give_local_immunity_traversal(VALUE obj)
{
    rb_gc_give_local_immunity_traversal_i(obj, NULL);
}

static VALUE
gc_deactivate_no_rest(rb_vm_t *vm)
{
    int old = vm->gc_deactivated;
    vm->gc_deactivated = true;
    return RBOOL(old);
}

void gc_rest_global(rb_objspace_t *objspace);

static int
mark_absorbed_threads_tbl_i(st_data_t key, st_data_t value, st_data_t data)
{
    rb_objspace_t *objspace = (rb_objspace_t *)data;
    VALUE th = ((rb_thread_t *) key)->self;
    if (objspace == GET_OBJSPACE_OF_VALUE(th)) {
	rb_gc_mark(th);
    }
    return ST_CONTINUE;
}

static void
mark_absorbed_threads_tbl(rb_objspace_t *objspace)
{
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->absorbed_thread_tbl_lock);
    if (global_space->absorbed_thread_tbl) {
	st_foreach(global_space->absorbed_thread_tbl, mark_absorbed_threads_tbl_i, (st_data_t)objspace);
    }
    rb_native_mutex_unlock(&global_space->absorbed_thread_tbl_lock);
}

static void
mark_zombie_threads(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    rb_native_mutex_lock(&local_data->zombie_threads_lock);
    if (!ccan_list_empty(&local_data->zombie_threads)) {
        rb_thread_t *zombie_th, *next_zombie_th;
        ccan_list_for_each_safe(&local_data->zombie_threads, zombie_th, next_zombie_th, sched.node.zombie_threads) {
            if (zombie_th->sched.finished) {
                ccan_list_del_init(&zombie_th->sched.node.zombie_threads);
            }
            else {
                rb_gc_mark(zombie_th->self);
            }
        }
    }
    rb_native_mutex_unlock(&local_data->zombie_threads_lock);
}

static void
heap_lock_enter(rb_objspace_t *objspace, rb_ractor_t *cr)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    if (local_data->heap_lock_owner != cr) {
	rb_native_mutex_lock(&local_data->heap_lock);
	local_data->heap_lock_owner = cr;
    }
    local_data->heap_lock_level++;
}

static void
heap_lock_leave(rb_objspace_t *objspace, rb_ractor_t *cr)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    VM_ASSERT(local_data->heap_lock_owner == cr);
    local_data->heap_lock_level--;
    if (local_data->heap_lock_level == 0) {
	local_data->heap_lock_owner = NULL;
	rb_native_mutex_unlock(&local_data->heap_lock);
    }
}

static bool
heap_locked(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    return local_data->heap_lock_owner == GET_RACTOR();
}

#define HEAP_LOCK_ENTER(objspace) { heap_lock_enter(objspace, GET_RACTOR());
#define HEAP_LOCK_LEAVE(objspace) heap_lock_leave(objspace, GET_RACTOR()); }

#define SUSPEND_HEAP_LOCK_BEGIN(objspace) { \
    bool heap_lock_suspended = false; \
    int _old_heap_lock_level; \
    struct objspace_local_data *_local_data = objspace_get_local_data(objspace); \
    if (_local_data->heap_lock_owner == GET_RACTOR()) { \
	heap_lock_suspended = true; \
	_old_heap_lock_level = _local_data->heap_lock_level; \
	_local_data->heap_lock_owner = NULL; \
	_local_data->heap_lock_level = 0; \
	rb_native_mutex_unlock(&_local_data->heap_lock); \
    }

#define SUSPEND_HEAP_LOCK_END(objspace) \
    if (heap_lock_suspended) { \
	rb_native_mutex_lock(&_local_data->heap_lock); \
	_local_data->heap_lock_owner = GET_RACTOR(); \
	_local_data->heap_lock_level = _old_heap_lock_level; \
    } \
}

#define LOCAL_GC_BEGIN(objspace) \
{ \
    SUSPEND_HEAP_LOCK_BEGIN(objspace); \
    begin_local_gc_section(GET_VM(), objspace, GET_RACTOR());

#define LOCAL_GC_END(objspace) \
    end_local_gc_section(GET_VM(), objspace, GET_RACTOR()); \
    SUSPEND_HEAP_LOCK_END(objspace); \
}

#define GLOBAL_GC_BEGIN(vm, objspace) \
{ \
    SUSPEND_HEAP_LOCK_BEGIN(objspace); \
    unsigned int _lev; \
    begin_global_gc_section(vm, objspace, &_lev); \


#define GLOBAL_GC_END(vm, objspace) \
    end_global_gc_section(vm, objspace, &_lev); \
    SUSPEND_HEAP_LOCK_END(objspace); \
}

static VALUE
rb_gc_deactivate(rb_vm_t *vm)
{
    VALUE old;
    GLOBAL_GC_BEGIN(vm, vm->objspace);
    {
	gc_rest_global(vm->objspace);
	old = gc_deactivate_no_rest(vm);
    }
    GLOBAL_GC_END(vm, vm->objspace);
    return old;
}

static const struct rb_callcache *
get_from_global_cc_cache_table(int index)
{
    rb_objspace_t *objspace = GET_OBJSPACE();
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    return local_data->global_cc_cache_table[index];
}

static void
set_in_global_cc_cache_table(int index, const struct rb_callcache *cc)
{
    rb_objspace_t *objspace = GET_OBJSPACE();
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    local_data->global_cc_cache_table[index] = cc;
}

static void
free_local_data_contents(struct objspace_local_data *local_data)
{
    if (!local_data->objspace_closed) {
	lock_ractor_set();
	ccan_list_del(&local_data->objspace_node);
	unlock_ractor_set();
    }

    st_free_table(local_data->id_to_obj_tbl);
    st_free_table(local_data->obj_to_id_tbl);
    rb_nativethread_lock_destroy(&local_data->obj_id_lock);

    st_free_table(local_data->shared_reference_tbl);
    rb_nativethread_lock_destroy(&local_data->shared_reference_tbl_lock);
    st_free_table(local_data->external_reference_tbl);
    rb_nativethread_lock_destroy(&local_data->external_reference_tbl_lock);

    st_free_table(local_data->received_obj_tbl);
    rb_nativethread_lock_destroy(&local_data->received_obj_tbl_lock);

    st_free_table(local_data->wmap_referenced_obj_tbl);
    rb_nativethread_lock_destroy(&local_data->wmap_referenced_obj_tbl_lock);

    st_free_table(local_data->contained_ractor_tbl);
    rb_nativethread_lock_destroy(&local_data->contained_ractor_tbl_lock);

    rb_nativethread_lock_destroy(&local_data->zombie_threads_lock);

    rb_nativethread_lock_destroy(&local_data->heap_lock);

    rb_nativethread_lock_destroy(&local_data->external_writebarrier_allowed_lock);
    rb_native_cond_destroy(&local_data->external_writebarrier_allowed_cond);
}

static void
rb_global_tables_init(void)
{
    rb_global_space_t *global_space = &rb_global_space;
    global_space->absorbed_thread_tbl = st_init_numtable();
}

struct received_obj_list {
    VALUE received_obj;
    struct received_obj_list *next;
};

static void
register_received_obj(rb_objspace_t *objspace, uintptr_t borrowing_id, VALUE obj)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->received_obj_tbl_lock);
    struct received_obj_list *new_item;
    new_item = ALLOC(struct received_obj_list);
    new_item->received_obj = obj;

    st_data_t data;
    int list_found = st_lookup(local_data->received_obj_tbl, (st_data_t)borrowing_id, &data);
    struct received_obj_list *lst = list_found ? (struct received_obj_list *)data : NULL;
    new_item->next = lst;

    st_insert_no_gc(local_data->received_obj_tbl, (st_data_t)GET_RACTOR()->borrowing_sync.borrowing_id, (st_data_t)new_item);
    rb_native_mutex_unlock(&local_data->received_obj_tbl_lock);
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
mark_received_received_obj_tbl(rb_objspace_t *objspace)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->received_obj_tbl_lock);
    st_foreach(local_data->received_obj_tbl, mark_received_obj_list, (st_data_t)objspace);
    rb_native_mutex_unlock(&local_data->received_obj_tbl_lock);
}

static void
remove_received_obj_list(rb_objspace_t *objspace, uintptr_t borrowing_id)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&local_data->received_obj_tbl_lock);
    st_data_t data;
    int list_found = st_lookup(local_data->received_obj_tbl, (st_data_t)borrowing_id, &data);
    if (list_found) {
	struct received_obj_list *p = (struct received_obj_list *)data;
	struct received_obj_list *next = NULL;
	while (p) {
	    next = p->next;
	    free(p);
	    p = next;
	}
    }

    st_delete(local_data->received_obj_tbl, &borrowing_id, NULL);
    rb_native_mutex_unlock(&local_data->received_obj_tbl_lock);
}

void rb_gc_safe_lock_enter(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_leave(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_initialize(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_destroy(rb_gc_safe_lock_t *gs_lock);
bool rb_gc_safe_lock_acquired(rb_gc_safe_lock_t *gs_lock);

struct borrowing_data_args {
    rb_ractor_t *borrower;
    rb_ractor_t *target_ractor;
    rb_ractor_t *old_target;
    VALUE (*func)(VALUE);
    VALUE func_args;
    uintptr_t old_borrowing_id;
};

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

    struct objspace_local_data *borrower_local_data = objspace_get_local_data(borrower->local_objspace);

    borrowing_data->old_target = borrower_local_data->alloc_target_ractor;
    borrower_local_data->alloc_target_ractor = target_ractor;

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

    struct objspace_local_data *borrower_local_data = objspace_get_local_data(borrower->local_objspace);

    rb_ractor_t *finished_target = borrower_local_data->alloc_target_ractor;
    VM_ASSERT(finished_target != borrower);
    uintptr_t finished_borrowing_id = borrower->borrowing_sync.borrowing_id;

    borrower_local_data->alloc_target_ractor = target_to_restore;
    borrower->borrowing_sync.borrowing_id = (uintptr_t)borrowing_data->old_borrowing_id;

    if (!!finished_target) {
	rb_borrowing_sync_lock(finished_target);
	if (LIKELY(!finished_target->borrowing_sync.borrowing_closed)) {
	    remove_received_obj_list(finished_target->local_objspace, finished_borrowing_id);
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
    rb_objspace_t *borrower_objspace = borrowing_data->borrower->local_objspace;
    if (!SPECIAL_CONST_P(result) && GET_OBJSPACE_OF_VALUE(result) != borrower_objspace) {
	VM_ASSERT(FL_TEST(result, FL_SHAREABLE));
	rb_register_new_external_reference(borrower_objspace, result);
    }
    return result;
}

VALUE rb_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args);

static struct heap_page *
current_borrowable_page(rb_ractor_t *r, int size_pool_idx)
{
    return r->newobj_borrowing_cache.size_pool_caches[size_pool_idx].using_page;
}

static void
lock_own_borrowable_page(rb_ractor_t *cr, int size_pool_idx)
{
    if (cr->borrowing_sync.page_lock_lev[size_pool_idx] == 0) {
	rb_native_mutex_lock(&cr->borrowing_sync.page_lock[size_pool_idx]);
	cr->borrowing_sync.page_lock_owner[size_pool_idx] = cr;
    }
    cr->borrowing_sync.page_lock_lev[size_pool_idx]++;
    cr->borrowing_sync.page_recently_locked[size_pool_idx] = true;
}

static void
unlock_own_borrowable_page(rb_ractor_t *cr, int size_pool_idx)
{
    cr->borrowing_sync.page_lock_lev[size_pool_idx]--;
    if (cr->borrowing_sync.page_lock_lev[size_pool_idx] == 0) {
	cr->borrowing_sync.page_lock_owner[size_pool_idx] = NULL;
	rb_native_mutex_unlock(&cr->borrowing_sync.page_lock[size_pool_idx]);
    }
}

static rb_objspace_t *
current_ractor_objspace(void)
{
    if (ruby_single_main_objspace) {
	return ruby_single_main_objspace;
    }

    rb_objspace_t *objspace = GET_OBJSPACE();
#ifdef RB_THREAD_LOCAL_SPECIFIER
    VM_ASSERT(objspace == rb_current_objspace_noinline());
#endif

    if (objspace && objspace_get_local_data(objspace)->global_gc_current_target) {
	objspace = objspace_get_local_data(objspace)->global_gc_current_target;
    }
    return objspace;
}
void rb_objspace_free_all_non_main(rb_vm_t *vm);

static void
decrement_global_allocated_pages(bool global_pages_locked)
{
    rb_global_space_t *global_space = &rb_global_space;
    if (!global_pages_locked) rb_native_mutex_lock(&global_space->all_pages.global_pages_lock);
    all_allocated_pages_global--;
    if (!global_pages_locked) rb_native_mutex_unlock(&global_space->all_pages.global_pages_lock);
}

static void
rb_ractor_sched_signal_possible_waiters(rb_vm_t *vm)
{
    rb_native_cond_broadcast(&vm->global_gc_finished);

    rb_global_space_t *global_space = &rb_global_space;
    rb_native_cond_broadcast(&global_space->absorption.mode_change_cond);

    rb_ractor_t *r = NULL;
    ccan_list_for_each(&vm->ractor.set, r, vmlr_node) {
	rb_native_cond_signal(&r->sync.close_cond);
	rb_native_cond_signal(&r->borrowing_sync.no_borrowers);
    }
}

static int
delete_from_obj_id_tables(VALUE obj, st_data_t *o, st_data_t *id)
{
    int deletion_success;
    WITH_OBJSPACE_LOCAL_DATA_ENTER(obj, local_data);
    {
	rb_native_mutex_lock(&local_data->obj_id_lock);
	deletion_success = st_delete(local_data->obj_to_id_tbl, o, id);
	if (deletion_success) {
	    VM_ASSERT(*id);
	    st_delete(local_data->id_to_obj_tbl, id, NULL);
	}
	rb_native_mutex_unlock(&local_data->obj_id_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(local_data);
    return deletion_success;
}

RUBY_EXTERN const struct st_hash_type object_id_hash_type;

static void
objspace_local_data_init(rb_objspace_t *objspace, struct objspace_local_data *local_data)
{
    local_data->objspace = objspace;

    local_data->id_to_obj_tbl = st_init_table(&object_id_hash_type);
    local_data->obj_to_id_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->obj_id_lock);
    local_data->local_gc_level = 0;

    local_data->finalizer_table = st_init_numtable();
    ccan_list_head_init(&local_data->zombie_threads);

    rb_nativethread_lock_initialize(&local_data->zombie_threads_lock);

    local_data->shared_reference_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->shared_reference_tbl_lock);
    local_data->external_reference_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->external_reference_tbl_lock);
    
    local_data->received_obj_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->received_obj_tbl_lock);

    local_data->wmap_referenced_obj_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->wmap_referenced_obj_tbl_lock);

    local_data->contained_ractor_tbl = st_init_numtable();
    rb_nativethread_lock_initialize(&local_data->contained_ractor_tbl_lock);

    rb_nativethread_lock_initialize(&local_data->heap_lock);
    local_data->heap_lock_owner = NULL;
    local_data->heap_lock_level = 0;

    local_data->external_writebarrier_allowed = true;
    rb_nativethread_lock_initialize(&local_data->external_writebarrier_allowed_lock);
    rb_native_cond_initialize(&local_data->external_writebarrier_allowed_cond);

    local_data->alloc_target_ractor = NULL;

    lock_ractor_set();
    ccan_list_add_tail(&GET_VM()->objspace_set, &local_data->objspace_node);
    unlock_ractor_set();
}

static void
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

static VALUE
lookup_id_in_objspace(rb_objspace_t *objspace, VALUE objid)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    VALUE orig;

    rb_native_mutex_lock(&local_data->obj_id_lock);
    int id_found = st_lookup(local_data->id_to_obj_tbl, objid, &orig);
    rb_native_mutex_unlock(&local_data->obj_id_lock);
    if (id_found) {
	return orig;
    }
    else {
	return Qundef;
    }
    return orig;
}

static VALUE
rb_gc_id2ref_obj_tbl(VALUE objid)
{
    rb_objspace_t *objspace = GET_OBJSPACE();

    VALUE result = lookup_id_in_objspace(objspace, objid);
    if (result != Qundef) {
	return result;
    }

    rb_vm_t *vm = GET_VM();

    lock_ractor_set();
    struct objspace_local_data *local_data = NULL;
    ccan_list_for_each(&vm->objspace_set, local_data, objspace_node) {
	rb_objspace_t *os = local_data->objspace;
	result = lookup_id_in_objspace(os, objid);
	if (result != Qundef) {
	    break;
	}
    }
    unlock_ractor_set();
    return result;
}

static bool
rb_nonexistent_id(VALUE objid)
{
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->next_object_id_lock);
    VALUE not_id_value = rb_int_ge(objid, ULL2NUM(global_space->next_object_id));
    rb_native_mutex_unlock(&global_space->next_object_id_lock);
    return !!not_id_value;
}

static VALUE
retrieve_next_obj_id(int increment)
{
    VALUE id;
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->next_object_id_lock);
    id = ULL2NUM(global_space->next_object_id);
    global_space->next_object_id += increment;
    global_space->prev_id_assigner = rb_current_allocating_ractor();
    rb_native_mutex_unlock(&global_space->next_object_id_lock);
    return id;
}

#if VM_CHECK_MODE > 0
bool rb_ractor_safe_gc_state(void);
#endif

#define ASSERT_ractor_safe_gc_state() VM_ASSERT(rb_ractor_safe_gc_state())

#if VM_CHECK_MODE > 0
static int
count_objspaces(rb_vm_t *vm) //TODO: Replace with count-tracker
{
    int i = 0;
    struct objspace_local_data *local_data = NULL;
    ccan_list_for_each(&vm->objspace_set, local_data, objspace_node) {
	i++;
    }
    return i;
}

static bool
shared_reference_tbl_empty(rb_objspace_t *objspace)
{
    bool empty;
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    rb_native_mutex_lock(&local_data->shared_reference_tbl_lock);
    empty = (st_table_size(local_data->shared_reference_tbl) == 0);
    rb_native_mutex_unlock(&local_data->shared_reference_tbl_lock);
    return empty;
}

static bool
external_reference_tbl_empty(rb_objspace_t *objspace)
{
    bool empty;
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    rb_native_mutex_lock(&local_data->external_reference_tbl_lock);
    empty = (st_table_size(local_data->external_reference_tbl) == 0);
    rb_native_mutex_unlock(&local_data->external_reference_tbl_lock);
    return empty;
}

#endif

void arrange_next_gc_global_status(double sharedobject_limit_factor);
bool global_gc_needed(void);

static void
global_gc_for_each_objspace(rb_vm_t *vm, rb_objspace_t *runner_objspace, void (gc_func)(rb_objspace_t *objspace))
{
    struct objspace_local_data *runner_data = objspace_get_local_data(runner_objspace);
    struct objspace_local_data *local_data = NULL;
    rb_objspace_t *prev_target = runner_data->global_gc_current_target;
    ccan_list_for_each(&vm->objspace_set, local_data, objspace_node) {
	rb_objspace_t *os = local_data->objspace;
	runner_data->global_gc_current_target = os;
	gc_func(os);
    }
    runner_data->global_gc_current_target = prev_target;
}

void rb_gc_writebarrier_safe_objspace(VALUE a, VALUE b, rb_objspace_t *objspace);
static void
gc_writebarrier_parallel_objspace(VALUE a, VALUE b, rb_objspace_t *objspace)
{
    struct objspace_local_data *data = objspace_get_local_data(objspace);
    rb_native_mutex_lock(&data->external_writebarrier_allowed_lock);
    while (!data->external_writebarrier_allowed) {
	rb_native_cond_wait(&data->external_writebarrier_allowed_cond, &data->external_writebarrier_allowed_lock);
    }
    rb_gc_writebarrier_safe_objspace(a, b, objspace);
    rb_native_mutex_unlock(&data->external_writebarrier_allowed_lock);
}

static void
rb_gc_writebarrier_multi_objspace(VALUE a, VALUE b, rb_objspace_t *current_objspace)
{
    VM_ASSERT(!SPECIAL_CONST_P(a));
    VM_ASSERT(!SPECIAL_CONST_P(b));

    WITH_OBJSPACE_OF_VALUE_ENTER(b, b_objspace);
    {
	if (FL_TEST_RAW(b, FL_SHAREABLE)) {
	    if (UNLIKELY(GET_OBJSPACE_OF_VALUE(a) != b_objspace)) {
		WITH_OBJSPACE_OF_VALUE_ENTER(a, a_objspace);
		{
		    if (LIKELY(a_objspace != b_objspace)) register_new_external_reference(a_objspace, b_objspace, b);
		}
		WITH_OBJSPACE_OF_VALUE_LEAVE(a_objspace);
	    }

	    if (LIKELY(b_objspace == current_objspace || b_objspace == rb_current_allocating_ractor()->local_objspace)) {
		rb_gc_writebarrier_safe_objspace(a, b, b_objspace);
	    }
	    else {
		gc_writebarrier_parallel_objspace(a, b, b_objspace);
	    }
	}
	else {
	    VM_ASSERT(b_objspace == current_objspace || b_objspace == rb_current_allocating_ractor()->local_objspace);
	    VM_ASSERT(GET_OBJSPACE_OF_VALUE(a) == b_objspace || GET_RACTOR()->during_ractor_copy);

	    rb_gc_writebarrier_safe_objspace(a, b, b_objspace);
	}
    }
    WITH_OBJSPACE_OF_VALUE_LEAVE(b_objspace);
}

#define POSSIBLE_USAGE_OF_BORROWING_PAGE_BEGIN(objspace, page) { \
    rb_ractor_t *_cr = GET_RACTOR(); \
    int _size_pool_idx = get_size_pool_idx(objspace, page->size_pool); \
    bool _using_borrowable_page = false; \
    if (!objspace->local_data.freeing_all) { \
	rb_borrowing_sync_lock(_cr); \
	if (current_borrowable_page(_cr, _size_pool_idx) == page) { \
	    lock_own_borrowable_page(_cr, _size_pool_idx); \
	    _using_borrowable_page = true; \
	} \
	rb_borrowing_sync_unlock(_cr); \
    }

#define POSSIBLE_USAGE_OF_BORROWING_PAGE_END() \
    if (_using_borrowable_page) { \
	unlock_own_borrowable_page(_cr, _size_pool_idx); \
    } \
}

typedef struct RVALUE RVALUE;
typedef struct rb_size_pool_struct rb_size_pool_t;
void total_allocated_objects_update(rb_objspace_t *objspace);
void update_obj_id_mapping(rb_objspace_t *objspace, RVALUE *dest, RVALUE *src);
bool in_marking_range(rb_objspace_t *objspace, VALUE obj);
void check_not_tnone(VALUE obj);
bool confirm_global_connections(rb_objspace_t *objspace, VALUE obj);
void size_pool_local_stats_init(rb_objspace_t *objspace, int size_pool_idx);
void objspace_local_stats_init(rb_objspace_t *objspace);
void gc_ractor_newobj_size_pool_cache_clear(rb_ractor_newobj_size_pool_cache_t *cache);
void gc_mark_reset_parent(rb_objspace_t *objspace);
int get_size_pool_idx(rb_objspace_t *objspace, rb_size_pool_t *size_pool);
void update_obj_id_refs(rb_objspace_t *objspace);

#endif /* RUBY_GLOSPACE_H */
