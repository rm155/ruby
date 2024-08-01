#include "glospace.h"

static void confirm_ogs_chain_node_removed(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node);
static void ractor_chain_release(struct rb_ractor_chain *ractor_chain);
static void ractor_chain_init(rb_global_space_t *global_space, struct rb_ractor_chain *ractor_chain);

rb_global_space_t *
get_global_space(void)
{
    return GET_VM()->global_space;
}

rb_global_space_t *
rb_global_space_init(void)
{
    rb_global_space_t *global_space = calloc(1, sizeof(rb_global_space_t));
    global_space->next_object_id = rb_get_obj_id_initial();
    global_space->prev_id_assigner = NULL;
    rb_nativethread_lock_initialize(&global_space->next_object_id_lock);
    rb_nativethread_lock_initialize(&global_space->absorbed_thread_tbl_lock);
    rb_nativethread_lock_initialize(&global_space->rglobalgc.shared_tracking_lock);
    ractor_chain_init(global_space, &global_space->ogs_tracking.ractor_chain);

    global_space->ogs_tracking.ractor_chain.node_removed_callback = confirm_ogs_chain_node_removed;

    rb_nativethread_lock_initialize(&global_space->absorption.mode_lock);
    rb_native_cond_initialize(&global_space->absorption.mode_change_cond);
    global_space->absorption.objspace_readers = 0;
    global_space->absorption.absorbers = 0;

    rb_nativethread_lock_initialize(&global_space->all_pages.global_pages_lock);
    return global_space;
}

void
rb_global_space_free(rb_global_space_t *global_space)
{
    rb_nativethread_lock_destroy(&global_space->next_object_id_lock);
    st_free_table(global_space->absorbed_thread_tbl);
    rb_nativethread_lock_destroy(&global_space->absorbed_thread_tbl_lock);
    rb_nativethread_lock_destroy(&global_space->rglobalgc.shared_tracking_lock);
    ractor_chain_release(&global_space->ogs_tracking.ractor_chain);

    rb_native_cond_destroy(&global_space->ogs_tracking.removal_cond);

    rb_nativethread_lock_destroy(&global_space->absorption.mode_lock);
    rb_native_cond_destroy(&global_space->absorption.mode_change_cond);

    rb_nativethread_lock_destroy(&global_space->all_pages.global_pages_lock);

    if (all_pages_sorted_global) {
        free(all_pages_sorted_global);
        all_allocated_pages_global = 0;
        all_pages_sorted_length_global = 0;
        all_pages_lomem_global = 0;
        all_pages_himem_global = 0;
    }

    free(global_space);
}

static void
confirm_ogs_chain_node_removed(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node)
{
    rb_global_space_t *global_space = ractor_chain->global_space;
    rb_native_cond_broadcast(&global_space->ogs_tracking.removal_cond);
}

static void
ractor_chain_release(struct rb_ractor_chain *ractor_chain)
{
    rb_nativethread_lock_destroy(&ractor_chain->lock);
}

static void
ractor_chain_init(rb_global_space_t *global_space, struct rb_ractor_chain *ractor_chain)
{
    ractor_chain->head_node = NULL;
    ractor_chain->tail_node = NULL;
    rb_nativethread_lock_initialize(&ractor_chain->lock);
    rb_native_cond_initialize(&global_space->ogs_tracking.removal_cond);

    ractor_chain->node_added_callback = NULL;
    ractor_chain->node_removed_callback = NULL;
    ractor_chain->global_space = global_space;
}


static void
add_ractor_chain_node(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node)
{
    rc_node->prev_node = NULL;
    rc_node->next_node = ractor_chain->head_node;

    if (rc_node->next_node) {
	VM_ASSERT(rc_node->next_node->prev_node == NULL);
	rc_node->next_node->prev_node = rc_node;
    }
    else {
	ractor_chain->tail_node = rc_node;
    }

    ractor_chain->head_node = rc_node;
    if (ractor_chain->node_added_callback) {
	ractor_chain->node_added_callback(ractor_chain, rc_node);
    }
}

static struct rb_ractor_chain_node *
create_node_in_chain(struct rb_ractor_chain *ractor_chain, rb_ractor_t *r, rb_atomic_t initial_value)
{
    rb_native_mutex_lock(&ractor_chain->lock);

    struct rb_ractor_chain_node *rc_node = malloc(sizeof(struct rb_ractor_chain_node));
    rc_node->value = initial_value;
    rc_node->ractor = r;
    add_ractor_chain_node(ractor_chain, rc_node);

    rb_native_mutex_unlock(&ractor_chain->lock);
    return rc_node;
}

void
rb_ractor_chains_register(rb_ractor_t *r)
{
    rb_global_space_t *global_space = &rb_global_space;
    r->ogs_chain_node = create_node_in_chain(&global_space->ogs_tracking.ractor_chain, r, OGS_FLAG_NONE);
    r->registered_in_ractor_chains = true;
}

static void
remove_ractor_chain_node(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node)
{
    struct rb_ractor_chain_node *prev_node = rc_node->prev_node;
    struct rb_ractor_chain_node *next_node = rc_node->next_node;

    if (prev_node) {
	VM_ASSERT(prev_node->next_node == rc_node);
	VM_ASSERT(ractor_chain->head_node != rc_node);
	prev_node->next_node = next_node;
    }
    else {
	VM_ASSERT(ractor_chain->head_node == rc_node);
	ractor_chain->head_node = next_node;
    }

    if (next_node) {
	VM_ASSERT(next_node->prev_node == rc_node);
	VM_ASSERT(ractor_chain->tail_node != rc_node);
	next_node->prev_node = prev_node;
    }
    else {
	VM_ASSERT(ractor_chain->tail_node == rc_node);
	ractor_chain->tail_node = prev_node;
    }
    if (ractor_chain->node_removed_callback) {
	ractor_chain->node_removed_callback(ractor_chain, rc_node);
    }
}

static void
delete_node_in_ractor_chain(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node)
{
    rb_native_mutex_lock(&ractor_chain->lock);

    remove_ractor_chain_node(ractor_chain, rc_node);
    free(rc_node);

    rb_native_mutex_unlock(&ractor_chain->lock);
}

void
rb_ractor_chains_unregister(rb_ractor_t *r)
{
    rb_global_space_t *global_space = &rb_global_space;
    delete_node_in_ractor_chain(&global_space->ogs_tracking.ractor_chain, r->ogs_chain_node);
    r->ogs_chain_node = NULL;
    r->registered_in_ractor_chains = false;
}

static void
ractor_chain_send_to_front(struct rb_ractor_chain *ractor_chain, struct rb_ractor_chain_node *rc_node)
{
    rb_native_mutex_lock(&ractor_chain->lock);

    remove_ractor_chain_node(ractor_chain, rc_node);
    add_ractor_chain_node(ractor_chain, rc_node);

    rb_native_mutex_unlock(&ractor_chain->lock);
}

void
rb_ractor_object_graph_safety_advance(rb_ractor_t *r, unsigned int reason)
{
    if (!r->ogs_chain_node) return;
    rb_atomic_t oldval = RUBY_ATOMIC_LOAD(r->ogs_chain_node->value);
    int newval = oldval | reason;
    ATOMIC_SET(r->ogs_chain_node->value, newval);
    if (oldval == 0 && newval != 0) {
	rb_global_space_t *global_space = &rb_global_space;
	ractor_chain_send_to_front(&global_space->ogs_tracking.ractor_chain, r->ogs_chain_node);
    }
}

void
rb_ractor_object_graph_safety_withdraw(rb_ractor_t *r, unsigned int reason)
{
    if (!r->ogs_chain_node) return;
    rb_atomic_t oldval = RUBY_ATOMIC_LOAD(r->ogs_chain_node->value);
    int newval = oldval & (~reason);
    ATOMIC_SET(r->ogs_chain_node->value, newval);
}

int
st_insert_no_gc(st_table *tab, st_data_t key, st_data_t value)
{
    VALUE already_disabled = rb_gc_disable_no_rest();
    st_insert(tab, key, value);
    if (already_disabled == Qfalse) rb_gc_enable();
}

static int
insert_table_row(st_data_t key, st_data_t val, st_data_t arg)
{
    st_table *tbl = (st_table *)arg;
    st_insert_no_gc(tbl, key, val);
    return ST_CONTINUE;
}

static void
absorb_table_contents(st_table *receiving_tbl, st_table *added_tbl)
{
    st_foreach(added_tbl, insert_table_row, (st_data_t)receiving_tbl);
}

static int
insert_external_reference_row(st_data_t key, st_data_t val, st_data_t arg)
{
    VALUE obj = (VALUE)key;
    gc_reference_status_t *rs = (gc_reference_status_t *)val;
    struct objspace_local_data **local_data = (struct objspace_local_data **)arg;
    struct objspace_local_data *local_data_to_update = local_data[0];
    struct objspace_local_data *local_data_to_copy_from = local_data[1];
    st_table *target_tbl = local_data_to_update->external_reference_tbl;

    VM_ASSERT(GET_OBJSPACE_OF_VALUE(obj) != local_data_to_update->objspace);
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
    struct rb_objspace **objspaces = (struct rb_objspace **)arg;

    struct rb_objspace *tbl_objspace = objspaces[0];
    struct rb_objspace *comparison_objspace = objspaces[1];
    struct rb_objspace *source_objspace = GET_OBJSPACE_OF_VALUE(obj);

    if (source_objspace == comparison_objspace) {
	drop_external_reference_usage(tbl_objspace, obj, rs);
	return ST_DELETE;
    }
    return ST_CONTINUE;
}

static void
prepare_for_reference_tbl_absorption(struct objspace_local_data *local_data_to_update, struct objspace_local_data *local_data_to_copy_from)
{
    struct rb_objspace *objspaces[2];
    objspaces[0] = local_data_to_copy_from->objspace;
    objspaces[1] = local_data_to_update->objspace;
    st_foreach(local_data_to_copy_from->external_reference_tbl, prepare_for_reference_tbl_absorption_i, (st_data_t)objspaces);

    objspaces[0] = local_data_to_update->objspace;
    objspaces[1] = local_data_to_copy_from->objspace;
    st_foreach(local_data_to_update->external_reference_tbl, prepare_for_reference_tbl_absorption_i, (st_data_t)objspaces);
}

static void
absorb_external_references(struct objspace_local_data *local_data_to_update, struct objspace_local_data *local_data_to_copy_from)
{
    struct objspace_local_data *local_data[2];
    local_data[0] = local_data_to_update;
    local_data[1] = local_data_to_copy_from;
    st_foreach(local_data_to_copy_from->external_reference_tbl, insert_external_reference_row, (st_data_t)local_data);
}

static void
absorb_shared_object_tables(struct objspace_local_data *local_data_to_update, struct objspace_local_data *local_data_to_copy_from)
{
    rb_native_mutex_lock(&local_data_to_copy_from->external_reference_tbl_lock);
    rb_native_mutex_lock(&local_data_to_update->external_reference_tbl_lock);

    prepare_for_reference_tbl_absorption(local_data_to_update, local_data_to_copy_from);
    absorb_external_references(local_data_to_update, local_data_to_copy_from);

    rb_native_mutex_unlock(&local_data_to_copy_from->external_reference_tbl_lock);
    rb_native_mutex_unlock(&local_data_to_update->external_reference_tbl_lock);

    rb_native_mutex_lock(&local_data_to_copy_from->shared_reference_tbl_lock);
    rb_native_mutex_lock(&local_data_to_update->shared_reference_tbl_lock);

    absorb_table_contents(local_data_to_update->shared_reference_tbl, local_data_to_copy_from->shared_reference_tbl);

    rb_native_mutex_unlock(&local_data_to_copy_from->shared_reference_tbl_lock);
    rb_native_mutex_unlock(&local_data_to_update->shared_reference_tbl_lock);
}

static void
absorb_objspace_tables(struct objspace_local_data *local_data_to_update, struct objspace_local_data *local_data_to_copy_from)
{
    //Finalizer table
    absorb_table_contents(local_data_to_update->finalizer_table, local_data_to_copy_from->finalizer_table);

    //Contained ractor table
    rb_native_mutex_lock(&local_data_to_copy_from->contained_ractor_tbl_lock);
    rb_native_mutex_lock(&local_data_to_update->contained_ractor_tbl_lock);

    absorb_table_contents(local_data_to_update->contained_ractor_tbl, local_data_to_copy_from->contained_ractor_tbl);

    rb_native_mutex_unlock(&local_data_to_copy_from->contained_ractor_tbl_lock);
    rb_native_mutex_unlock(&local_data_to_update->contained_ractor_tbl_lock);

    //Shared object tables
    absorb_shared_object_tables(local_data_to_update, local_data_to_copy_from);

    rb_native_mutex_lock(&local_data_to_copy_from->wmap_referenced_obj_tbl_lock);
    rb_native_mutex_lock(&local_data_to_update->wmap_referenced_obj_tbl_lock);

    absorb_table_contents(local_data_to_update->wmap_referenced_obj_tbl, local_data_to_copy_from->wmap_referenced_obj_tbl);

    rb_native_mutex_unlock(&local_data_to_copy_from->wmap_referenced_obj_tbl_lock);
    rb_native_mutex_unlock(&local_data_to_update->wmap_referenced_obj_tbl_lock);

    //Object ID tables
    rb_native_mutex_lock(&local_data_to_copy_from->obj_id_lock);
    rb_native_mutex_lock(&local_data_to_update->obj_id_lock);

    absorb_table_contents(local_data_to_update->obj_to_id_tbl, local_data_to_copy_from->obj_to_id_tbl);
    absorb_table_contents(local_data_to_update->id_to_obj_tbl, local_data_to_copy_from->id_to_obj_tbl);

    rb_native_mutex_unlock(&local_data_to_copy_from->obj_id_lock);
    rb_native_mutex_unlock(&local_data_to_update->obj_id_lock);
}

static void
transfer_zombie_threads(struct objspace_local_data *receiving_data, struct objspace_local_data *closing_data)
{
    rb_native_mutex_lock(&receiving_data->zombie_threads_lock);
    //TODO: Assert that the closing objspace has had its pages absorbed (so locking is not needed)

    if (!ccan_list_empty(&closing_data->zombie_threads)) {
        rb_thread_t *zombie_th, *next_zombie_th;
        ccan_list_for_each_safe(&closing_data->zombie_threads, zombie_th, next_zombie_th, sched.node.zombie_threads) {
	    ccan_list_del_init(&zombie_th->sched.node.zombie_threads);
	    ccan_list_add(&receiving_data->zombie_threads, &zombie_th->sched.node.zombie_threads);
        }
    }

    rb_native_mutex_unlock(&receiving_data->zombie_threads_lock);
}

void
perform_absorption(struct objspace_local_data *receiving_data, struct objspace_local_data *closing_data)
{
    absorb_objspace_tables(receiving_data, closing_data);
    rb_objspace_absorb_heaps(receiving_data, closing_data);
    transfer_zombie_threads(receiving_data, closing_data);
}

void
close_objspace(struct objspace_local_data *local_data)
{
    lock_ractor_set();

    ccan_list_del(&local_data->objspace_node);
    local_data->objspace_closed = true;

    unlock_ractor_set();
}

static void
objspace_local_stats_transfer(struct objspace_local_stats *stats_to_update, struct objspace_local_stats *stats_to_copy_from)
{
    VM_ASSERT(stats_to_update->field_count == stats_to_copy_from->field_count);
    for (int i = 0; i < stats_to_update->field_count; i++) {
	*stats_to_update->stats[i] += *stats_to_copy_from->stats[i];
    }
}

void
update_objspace_counts(struct objspace_local_data *local_data_to_update, struct objspace_local_data *local_data_to_copy_from)
{
    objspace_local_stats_transfer(&local_data_to_update->gc_stats, &local_data_to_copy_from->gc_stats);

    for (int i = 0; i < SIZE_POOL_COUNT; i++) {
	objspace_local_stats_transfer(&local_data_to_update->size_pool_stats[i], &local_data_to_copy_from->size_pool_stats[i]);
    }
}

void
allocatable_pages_update_for_transfer(rb_objspace_t *receiving_objspace, rb_objspace_t *closing_objspace, bool eden)
{
    size_t additional_pages[SIZE_POOL_COUNT];
    for (int i = 0; i < SIZE_POOL_COUNT; i++) {
	additional_pages[i] = total_pages_in_heap(select_heap(closing_objspace, i, eden));
    }

    size_pool_allocatable_pages_update(receiving_objspace, additional_pages);
}

void
merge_freepages(rb_objspace_t *receiving_objspace, rb_objspace_t *closing_objspace, int size_pool_idx, bool eden)
{
    rb_heap_t *closing_heap = select_heap(closing_objspace, size_pool_idx, eden);
    rb_heap_t *receiving_heap = select_heap(receiving_objspace, size_pool_idx, eden);
    
    heap_append_free_page(receiving_heap, get_freepages(closing_heap));
}

void
transfer_size_pool(rb_objspace_t *receiving_objspace, rb_objspace_t *closing_objspace, int size_pool_idx, bool eden)
{
    rb_objspace_t *objspace = closing_objspace;
    struct heap_page *page = heap_get_top_page(select_heap(objspace, size_pool_idx, eden));
    while (page) {
	absorb_page_into_objspace(receiving_objspace, page, size_pool_idx, eden);
	page = heap_get_top_page(select_heap(objspace, size_pool_idx, eden));
    }
    merge_freepages(receiving_objspace, closing_objspace, size_pool_idx, eden);
}

void
transfer_all_size_pools(rb_objspace_t *receiving_objspace, rb_objspace_t *closing_objspace, bool eden)
{
    for (int i = 0; i < SIZE_POOL_COUNT; i++) {
	transfer_size_pool(receiving_objspace, closing_objspace, i, eden);
    }
}

void
rb_objspace_absorb_heaps(struct objspace_local_data *receiving_data, struct objspace_local_data *closing_data)
{
    allocatable_pages_update_for_transfer(receiving_data->objspace, closing_data->objspace, true);
    allocatable_pages_update_for_transfer(receiving_data->objspace, closing_data->objspace, false);
    transfer_all_size_pools(receiving_data->objspace, closing_data->objspace, true);
    transfer_all_size_pools(receiving_data->objspace, closing_data->objspace, false);
    //rb_gc_ractor_newobj_cache_clear(closing_data->ractor->newobj_cache);
    //rb_gc_ractor_newobj_cache_clear(closing_data->ractor->newobj_borrowing_cache);
    update_objspace_counts(receiving_data, closing_data);
}
void
objspace_absorb_enter(rb_global_space_t *global_space)
{
    rb_native_mutex_lock(&global_space->absorption.mode_lock);

    ASSERT_vm_locking();
    VM_ASSERT(global_space->absorption.objspace_readers == 0 || global_space->absorption.absorbers == 0);

    COND_AND_BARRIER_WAIT(GET_VM(), global_space->absorption.mode_lock, global_space->absorption.mode_change_cond, global_space->absorption.objspace_readers == 0);

    global_space->absorption.absorbers++;
    rb_native_mutex_unlock(&global_space->absorption.mode_lock);
}

void
objspace_absorb_leave(rb_global_space_t *global_space)
{
    rb_native_mutex_lock(&global_space->absorption.mode_lock);

    VM_ASSERT(global_space->absorption.objspace_readers == 0 || global_space->absorption.absorbers == 0);

    global_space->absorption.absorbers--;
    if (global_space->absorption.absorbers == 0) {
	rb_native_cond_broadcast(&global_space->absorption.mode_change_cond);
    }

    rb_native_mutex_unlock(&global_space->absorption.mode_lock);
}

void
rb_absorb_objspace_of_closing_ractor(rb_ractor_t *receiving_ractor, rb_ractor_t *closing_ractor)
{ 
    rb_global_space_t *global_space = &rb_global_space;

    struct objspace_local_data *receiving_data = objspace_get_local_data(receiving_ractor->local_objspace);
    struct objspace_local_data *closing_data = objspace_get_local_data(closing_ractor->local_objspace);

    rb_ractor_object_graph_safety_advance(receiving_data->ractor, OGS_FLAG_ABSORBING_OBJSPACE);

    VALUE already_disabled = rb_objspace_gc_disable(receiving_data->objspace);
    RB_VM_LOCK();
    {

    VM_COND_AND_BARRIER_WAIT(GET_VM(), closing_ractor->sync.close_cond, closing_ractor->sync.ready_to_close);

    objspace_absorb_enter(global_space);

    receiving_data->currently_absorbing = true;

    rb_borrowing_sync_lock(receiving_ractor);
    HEAP_LOCK_ENTER(receiving_data->objspace);
    {
	HEAP_LOCK_ENTER(closing_data->objspace);
	{
	    perform_absorption(receiving_data, closing_data);
	}
	HEAP_LOCK_LEAVE(closing_data->objspace);
    }
    HEAP_LOCK_LEAVE(receiving_data->objspace);

    close_objspace(closing_data);

    if (rb_ractor_status_p(closing_ractor, ractor_terminated)) {
	rb_remove_from_contained_ractor_tbl(closing_ractor);
	rb_objspace_free(closing_data->objspace);
	closing_ractor->local_objspace = NULL;
    }
    rb_borrowing_sync_unlock(receiving_ractor);

    receiving_data->currently_absorbing = false;

    closing_ractor->objspace_absorbed = true;
    objspace_absorb_leave(global_space);
    }
    RB_VM_UNLOCK();

    absorption_finished(receiving_data->objspace);

    rb_native_mutex_lock(&global_space->next_object_id_lock);
    if (global_space->prev_id_assigner == closing_ractor) {
	global_space->prev_id_assigner = receiving_ractor;
    }
    rb_native_mutex_unlock(&global_space->next_object_id_lock);

    if (already_disabled == Qfalse) rb_objspace_gc_enable(receiving_data->objspace);

    rb_ractor_object_graph_safety_withdraw(receiving_data->ractor, OGS_FLAG_ABSORBING_OBJSPACE);
}

void
rb_add_to_absorbed_threads_tbl(rb_thread_t *th)
{
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->absorbed_thread_tbl_lock);
    st_insert_no_gc(global_space->absorbed_thread_tbl, (st_data_t)th, INT2FIX(0));
    rb_native_mutex_unlock(&global_space->absorbed_thread_tbl_lock);
}

void
rb_remove_from_absorbed_threads_tbl(rb_thread_t *th)
{
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->absorbed_thread_tbl_lock);
    st_delete(global_space->absorbed_thread_tbl, (st_data_t *) &th, NULL);
    rb_native_mutex_unlock(&global_space->absorbed_thread_tbl_lock);
}

void
rb_add_zombie_thread(rb_thread_t *th)
{
    rb_vm_t *vm = th->vm;
    th->sched.finished = false;

    WITH_OBJSPACE_LOCAL_DATA_ENTER(th->self, data);
    {
	rb_native_mutex_lock(&data->zombie_threads_lock);
	ccan_list_add(&data->zombie_threads, &th->sched.node.zombie_threads);
	rb_native_mutex_unlock(&data->zombie_threads_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(data);
}

void
rb_add_to_contained_ractor_tbl(rb_ractor_t *r)
{
    VALUE ractor_obj = r->pub.self;
    WITH_OBJSPACE_LOCAL_DATA_ENTER(ractor_obj, data);
    {
	rb_native_mutex_lock(&data->contained_ractor_tbl_lock);
	st_insert_no_gc(data->contained_ractor_tbl, (st_data_t)ractor_obj, INT2FIX(0));
	rb_native_mutex_unlock(&data->contained_ractor_tbl_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(data);
}

void
rb_remove_from_contained_ractor_tbl(rb_ractor_t *r)
{
    VALUE ractor_obj = r->pub.self;
    WITH_OBJSPACE_LOCAL_DATA_ENTER(ractor_obj, data);
    {
	rb_native_mutex_lock(&data->contained_ractor_tbl_lock);
	st_delete(data->contained_ractor_tbl, (st_data_t *) &ractor_obj, NULL);
	rb_native_mutex_unlock(&data->contained_ractor_tbl_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(data);
}

void
rb_register_new_external_wmap_reference(VALUE *ptr)
{
    VALUE obj = *ptr;
    if (SPECIAL_CONST_P(obj)) return;

    WITH_OBJSPACE_LOCAL_DATA_ENTER(obj, data);
    {
	rb_native_mutex_lock(&data->wmap_referenced_obj_tbl_lock);
	st_insert_no_gc(data->wmap_referenced_obj_tbl, ptr, INT2FIX(0));
	rb_native_mutex_unlock(&data->wmap_referenced_obj_tbl_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(data);
}

void
rb_remove_from_external_weak_tables(VALUE *ptr)
{
    VALUE obj = *ptr;
    if (obj == Qundef || SPECIAL_CONST_P(obj) || !FL_TEST_RAW(obj, FL_SHAREABLE)) return;

    WITH_OBJSPACE_LOCAL_DATA_ENTER(obj, data);
    {
	rb_native_mutex_lock(&data->wmap_referenced_obj_tbl_lock);
	st_delete(data->wmap_referenced_obj_tbl, &ptr, NULL);
	rb_native_mutex_unlock(&data->wmap_referenced_obj_tbl_lock);
    }
    WITH_OBJSPACE_LOCAL_DATA_LEAVE(data);
}

static void
mark_global_cc_cache_table_item(rb_objspace_t *objspace, int index)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);
    const struct rb_callcache *cc = local_data->global_cc_cache_table[index];

    if (cc != NULL) {
	if (!vm_cc_invalidated_p(cc)) {
	    rb_gc_mark((VALUE)cc);
	}
	else {
	    local_data->global_cc_cache_table[index] = NULL;
	}
    }
}

void
mark_global_cc_cache_table(rb_objspace_t *objspace)
{
    for (int i=0; i<VM_GLOBAL_CC_CACHE_TABLE_SIZE; i++) {
	mark_global_cc_cache_table_item(objspace, i);
    }
}

void
rb_register_new_external_reference(rb_objspace_t *receiving_objspace, VALUE obj)
{
    if (RB_SPECIAL_CONST_P(obj)) return;
    WITH_OBJSPACE_OF_VALUE_ENTER(obj, source_objspace);
    {
	if (source_objspace != receiving_objspace) register_new_external_reference(receiving_objspace, source_objspace, obj);
    }
    WITH_OBJSPACE_OF_VALUE_LEAVE(source_objspace);
}

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

bool
rb_gc_safe_lock_acquired(rb_gc_safe_lock_t *gs_lock)
{
    return gs_lock->lock_owner == GET_RACTOR();
}

void
begin_local_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, rb_ractor_t *cr)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    if (!local_data->running_global_gc && local_data->local_gc_level == 0) {
	rb_ractor_object_graph_safety_advance(cr, OGS_FLAG_RUNNING_LOCAL_GC);
	rb_ractor_borrowing_barrier_begin(local_data->ractor);
    }

    local_data->local_gc_level++;

    rb_native_mutex_lock(&local_data->external_writebarrier_allowed_lock);
    local_data->external_writebarrier_allowed = false;
    rb_native_mutex_unlock(&local_data->external_writebarrier_allowed_lock);
}

void
end_local_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, rb_ractor_t *cr)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    rb_native_mutex_lock(&local_data->external_writebarrier_allowed_lock);
    local_data->external_writebarrier_allowed = true;
    rb_native_mutex_unlock(&local_data->external_writebarrier_allowed_lock);
    rb_native_cond_broadcast(&local_data->external_writebarrier_allowed_cond);

    local_data->local_gc_level--;

    if (!local_data->running_global_gc && local_data->local_gc_level == 0) {
	rb_ractor_borrowing_barrier_end(local_data->ractor);
	rb_ractor_object_graph_safety_withdraw(cr, OGS_FLAG_RUNNING_LOCAL_GC);
    }
}

void
begin_global_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, unsigned int *lev)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    rb_ractor_t *cr = GET_RACTOR();
    rb_ractor_object_graph_safety_advance(cr, OGS_FLAG_RUNNING_GLOBAL_GC);
    RB_VM_LOCK_ENTER_LEV(lev);
    VM_COND_AND_BARRIER_WAIT(vm, vm->global_gc_finished, !vm->global_gc_underway);
    local_data->running_global_gc = true;
    vm->global_gc_underway = true;
    rb_vm_barrier();
}

void
end_global_gc_section(rb_vm_t *vm, rb_objspace_t *objspace, unsigned int *lev)
{
    struct objspace_local_data *local_data = objspace_get_local_data(objspace);

    local_data->running_global_gc = false;
    vm->global_gc_underway = false;
    rb_native_cond_broadcast(&vm->global_gc_finished);
    RB_VM_LOCK_LEAVE_LEV(lev);
    rb_ractor_object_graph_safety_withdraw(GET_RACTOR(), OGS_FLAG_RUNNING_GLOBAL_GC);
}

bool
rb_redirecting_allocation(void)
{
    rb_ractor_t *r = ruby_single_main_ractor ? ruby_single_main_ractor : GET_RACTOR();
    if (r->local_objspace && objspace_get_local_data(r->local_objspace)->alloc_target_ractor) {
	return true;
    }
    return false;
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

void
rb_borrowing_status_pause(rb_ractor_t *cr)
{
    for (struct borrowing_target_node_t *btn = cr->borrowing_target_top; !!btn; btn = btn->next) {
	borrowing_count_decrement(btn->target_ractor);
    }
}

void
rb_borrowing_status_resume(rb_ractor_t *cr)
{
    for (struct borrowing_target_node_t *btn = cr->borrowing_target_top; !!btn; btn = btn->next) {
	borrowing_count_increment(btn->target_ractor);
    }
}

rb_ractor_t *
rb_current_allocating_ractor(void)
{
    rb_ractor_t *r = ruby_single_main_ractor ? ruby_single_main_ractor : GET_RACTOR();
    if (r->local_objspace && objspace_get_local_data(r->local_objspace)->alloc_target_ractor) {
	return objspace_get_local_data(r->local_objspace)->alloc_target_ractor;
    }
    return r;
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

void
rb_objspace_free_all_non_main(rb_vm_t *vm) {
    struct objspace_local_data *local_data = NULL;
    ccan_list_for_each(&GET_VM()->objspace_set, local_data, objspace_node) {
	if (local_data->objspace != vm->objspace) {
	    rb_objspace_free(local_data->objspace);
	}
    }
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

void
arrange_next_gc_global_status(double sharedobject_limit_factor)
{
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->rglobalgc.shared_tracking_lock);

    if (rb_during_global_gc()) {
	global_space->rglobalgc.shared_objects_limit = (size_t)(global_space->rglobalgc.shared_objects_total * sharedobject_limit_factor);
    }
    else if (rb_multi_ractor_p() && global_space->rglobalgc.shared_objects_total > global_space->rglobalgc.shared_objects_limit) {
	global_space->rglobalgc.need_global_gc = true;
    }

    rb_native_mutex_unlock(&global_space->rglobalgc.shared_tracking_lock);
}

bool
global_gc_needed(void)
{
    bool needed;
    rb_global_space_t *global_space = &rb_global_space;
    rb_native_mutex_lock(&global_space->rglobalgc.shared_tracking_lock);
    needed = global_space->rglobalgc.need_global_gc;
    rb_native_mutex_unlock(&global_space->rglobalgc.shared_tracking_lock);
    return needed;
}
