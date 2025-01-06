#ifndef RUBY_OBJSPACE_COORDINATOR_H
#define RUBY_OBJSPACE_COORDINATOR_H

#include "internal/gc.h"

typedef struct rb_ractor_struct rb_ractor_t; /* in vm_core.h */
struct rb_objspace_coordinator;

typedef struct rb_objspace_coordinator {
    unsigned long long next_object_id;

    rb_ractor_t *prev_id_assigner;
    rb_nativethread_lock_t next_object_id_lock;

    st_table *absorbed_thread_tbl;
    rb_nativethread_lock_t absorbed_thread_tbl_lock;

    rb_nativethread_cond_t global_gc_finished;
    bool global_gc_underway;

    unsigned int gc_deactivated: 1;

    struct {
        bool need_global_gc;
	size_t global_gc_threshold;
	size_t shared_objects_total;
	rb_nativethread_lock_t shared_tracking_lock;
    } rglobalgc;

    struct {
	rb_nativethread_lock_t mode_lock;
	rb_nativethread_cond_t mode_change_cond;
	int objspace_readers;
	int absorbers;
    } absorption;

} rb_objspace_coordinator_t;

struct rb_objspace *gc_current_objspace(void);
rb_objspace_coordinator_t *rb_get_objspace_coordinator(void);
rb_objspace_coordinator_t *rb_objspace_coordinator_init(void);
void rb_objspace_coordinator_free(rb_objspace_coordinator_t *objspace_coordinator);
VALUE object_id_global_search(VALUE objid);
bool rb_nonexistent_id(VALUE objid);
VALUE retrieve_next_obj_id(int increment);
void rb_objspace_free_all_non_main(rb_vm_t *vm);
void rb_ractor_sched_signal_possible_waiters(rb_objspace_coordinator_t *coordinator);
void rb_barrier_join_within_vm_lock(rb_vm_t *vm, rb_ractor_t *cr);

#if VM_CHECK_MODE > 0
int count_objspaces(rb_vm_t *vm); //TODO: Replace with count-tracker
#endif

#ifdef RUBY_THREAD_PTHREAD_H
# define BARRIER_WAITING(vm) (vm->ractor.sched.barrier_waiting)
#else
# define BARRIER_WAITING(vm) (vm->ractor.sync.barrier_waiting)
#endif

#define VM_COND_AND_BARRIER_WAIT(vm, cond, state_to_check) do { \
    ASSERT_vm_locking(); \
    rb_ractor_t *_cr = GET_RACTOR(); \
    while (BARRIER_WAITING(vm)) { \
	rb_barrier_join_within_vm_lock(vm, _cr); \
    } \
    bool _need_repeat = true; \
    if(state_to_check) { \
	_need_repeat = false; \
    } \
    else { \
	rb_vm_cond_wait(vm, &cond); \
	_need_repeat = !state_to_check || BARRIER_WAITING(vm); \
    } \
    if (!_need_repeat) break; \
} while(true)

#define COND_AND_BARRIER_WAIT(vm, lock, cond, state_to_check) do { \
    ASSERT_vm_locking(); \
    rb_ractor_t *_cr = GET_RACTOR(); \
    while (BARRIER_WAITING(vm)) { \
	rb_native_mutex_unlock(&lock); \
	rb_barrier_join_within_vm_lock(vm, _cr); \
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
    if (!_need_repeat) break; \
} while(true)


enum {
    shared_object_local,
    shared_object_unmarked,
    shared_object_marked,
    shared_object_discovered_and_marked,
    shared_object_added_externally,
};

typedef struct gc_reference_status {
    rb_atomic_t *refcount;
    int status;
} gc_reference_status_t;

typedef struct rb_objspace_gate {
    VALUE self;

    //Main connections
    rb_ractor_t *ractor;
    struct rb_objspace *objspace;
    struct ccan_list_node gate_node;

    //Local data
    st_table *shared_reference_tbl;
    rb_nativethread_lock_t shared_reference_tbl_lock;

    st_table *external_reference_tbl;
    rb_nativethread_lock_t external_reference_tbl_lock;

    st_table *local_immune_tbl;
    rb_nativethread_lock_t local_immune_tbl_lock;
    unsigned int local_immune_count;

    st_table *wmap_referenced_obj_tbl;
    rb_nativethread_lock_t wmap_referenced_obj_tbl_lock;

    st_table *contained_ractor_tbl;
    rb_nativethread_lock_t contained_ractor_tbl_lock;

    st_table *received_obj_tbl;
    rb_nativethread_lock_t received_obj_tbl_lock;

    struct ccan_list_head zombie_threads;
    rb_nativethread_lock_t zombie_threads_lock;

    const struct rb_callcache *objspace_cc_cache_table[VM_OBJSPACE_CC_CACHE_TABLE_SIZE]; // vm_eval.c

    //GC state
    bool running_global_gc;
    int local_gc_level;
    struct rb_objspace *gc_target;
    struct rb_objspace *current_parent_objspace;
    struct gc_mark_func_data_struct {
        void *data;
        void (*mark_func)(VALUE v, void *data);
    } *mark_func_data;


    //Objspace state
    bool objspace_closed;
    bool currently_absorbing;
    bool belong_to_single_main_ractor;
    bool freeing_all;

    bool running_local_gc;
    rb_nativethread_lock_t running_local_gc_lock;
    rb_nativethread_cond_t local_gc_stop_cond;

    rb_nativethread_lock_t objspace_lock;
    rb_ractor_t *objspace_lock_owner;
    int objspace_lock_level;

    rb_ractor_t *alloc_target_ractor;

    bool marking_machine_context;

} rb_objspace_gate_t;

#ifdef RB_THREAD_LOCAL_SPECIFIER
  #ifdef __APPLE__
#define GET_OBJSPACE_GATE() rb_current_os_gate()
  #else
#define GET_OBJSPACE_GATE() ruby_current_os_gate
  #endif
#else
#define GET_OBJSPACE_GATE() native_tls_get(ruby_current_os_gate_key)
#endif

#define GET_RACTOR_OF_VALUE(x)   (rb_gc_ractor_of_value(x))
#define GET_OBJSPACE_OF_VALUE(x) (rb_gc_objspace_of_value(x))

#define WITH_OBJSPACE_OF_VALUE_ENTER(obj, objspace) { \
    bool _using_objspace_read = false; \
    rb_objspace_coordinator_t *_objspace_coordinator; \
    struct rb_objspace *objspace = GET_OBJSPACE_OF_VALUE(obj); \
    if (UNLIKELY(!ruby_single_main_objspace && objspace != GET_RACTOR()->local_objspace)) { \
	_objspace_coordinator = rb_get_objspace_coordinator(); \
	rb_objspace_read_enter(_objspace_coordinator); \
	objspace = GET_OBJSPACE_OF_VALUE(obj); \
	_using_objspace_read = true; \
    }

#define WITH_OBJSPACE_OF_VALUE_LEAVE(objspace) \
    if (_using_objspace_read) { \
	rb_objspace_read_leave(_objspace_coordinator); \
    } \
}

#define WITH_OBJSPACE_GATE_ENTER(obj, data) { \
    rb_objspace_gate_t *data; \
    struct rb_objspace *_objspace_of_value; \
    WITH_OBJSPACE_OF_VALUE_ENTER(obj, _objspace_of_value); \
    { \
	data = rb_gc_local_gate_of_objspace(_objspace_of_value);

#define WITH_OBJSPACE_GATE_LEAVE(data) \
    } \
    WITH_OBJSPACE_OF_VALUE_LEAVE(_objspace_of_value); \
}

rb_objspace_gate_t *rb_objspace_gate_init(struct rb_objspace *objspace);
bool rb_obj_is_main_os_gate(VALUE obj);
int st_insert_no_gc(st_table *tab, st_data_t key, st_data_t value);
void rb_register_new_external_reference(rb_objspace_gate_t *receiving_os_gate, VALUE obj);
void confirm_externally_added_external_references(rb_objspace_gate_t *local_gate);
void mark_in_external_reference_tbl(rb_objspace_gate_t *os_gate, VALUE obj);
bool rb_external_reference_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj);
void update_shared_object_references(rb_objspace_gate_t *os_gate);
bool rb_shared_reference_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj);

#if VM_CHECK_MODE > 0
bool shared_reference_tbl_empty(rb_objspace_gate_t *os_gate);
bool external_reference_tbl_empty(rb_objspace_gate_t *os_gate);
#endif

void add_local_immune_object(VALUE obj);
void update_local_immune_tbl(rb_objspace_gate_t *os_gate);
bool rb_local_immune_tbl_contains(rb_objspace_gate_t *os_gate, VALUE obj, bool lock_needed);
unsigned int local_immune_objects_global_count(void);
void rb_local_immune_tbl_activate(void);

void add_reachable_objects_to_local_immune_tbl(VALUE obj);
#if VM_CHECK_MODE > 0
void verify_reachable_objects_in_local_immune_tbl(VALUE obj);
#endif

void rb_add_zombie_thread(rb_thread_t *th);
void mark_zombie_threads(rb_objspace_gate_t *os_gate);
void rb_add_to_contained_ractor_tbl(rb_ractor_t *r);
void rb_remove_from_contained_ractor_tbl(rb_ractor_t *r);
void mark_contained_ractor_tbl(rb_objspace_gate_t *os_gate);
void rb_register_received_obj(rb_objspace_gate_t *os_gate, uintptr_t borrowing_id, VALUE obj);
void rb_register_new_external_wmap_reference(VALUE *ptr);
void rb_remove_from_external_weak_tables(VALUE *ptr);
void gc_update_external_weak_references(rb_objspace_gate_t *os_gate);
const struct rb_callcache * get_from_objspace_cc_cache_table(rb_execution_context_t *ec, int index);
void set_in_objspace_cc_cache_table(rb_execution_context_t *ec, int index, const struct rb_callcache *cc);
void mark_objspace_cc_cache_table(rb_objspace_gate_t *os_gate);

void rb_objspace_read_enter(rb_objspace_coordinator_t *objspace_coordinator);
void rb_objspace_read_leave(rb_objspace_coordinator_t *objspace_coordinator);
void absorb_table_contents(st_table *receiving_tbl, st_table *added_tbl);
void rb_absorb_objspace_of_closing_ractor(rb_ractor_t *receiving_ractor, rb_ractor_t *closing_ractor);
void mark_absorbed_threads_tbl(rb_objspace_gate_t *os_gate);
void rb_add_to_absorbed_threads_tbl(rb_thread_t *th);
void rb_remove_from_absorbed_threads_tbl(rb_thread_t *th);


struct rb_borrowing_location_lock {
    rb_nativethread_lock_t page_lock;
    rb_ractor_t *page_lock_owner;
    int page_lock_lev;
    bool page_recently_locked;
};

VALUE rb_attempt_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args, bool *borrowing_success);
VALUE rb_run_with_redirected_allocation(rb_ractor_t *target_ractor, VALUE (*func)(VALUE), VALUE func_args);
rb_ractor_t *rb_current_allocating_ractor(void);
bool rb_redirecting_allocation(void);
void lock_own_borrowable_page(rb_ractor_t *cr, struct rb_borrowing_location_lock *location_lock);
void unlock_own_borrowable_page(rb_ractor_t *cr, struct rb_borrowing_location_lock *location_lock);
void borrowing_location_lock_init(struct rb_borrowing_location_lock *location_lock);
void borrowing_location_lock_release(struct rb_borrowing_location_lock *location_lock);

void rb_gc_safe_lock_enter(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_leave(rb_gc_safe_lock_t *gs_lock);
bool rb_gc_safe_lock_acquired(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_initialize(rb_gc_safe_lock_t *gs_lock);
void rb_gc_safe_lock_destroy(rb_gc_safe_lock_t *gs_lock);

void objspace_lock_enter(rb_objspace_gate_t *os_gate, rb_ractor_t *cr);
void objspace_lock_leave(rb_objspace_gate_t *os_gate, rb_ractor_t *cr);
bool objspace_locked(rb_objspace_gate_t *os_gate);


#define OBJSPACE_LOCK_ENTER(objspace) { objspace_lock_enter(rb_gc_local_gate_of_objspace(objspace), GET_RACTOR());
#define OBJSPACE_LOCK_LEAVE(objspace) objspace_lock_leave(rb_gc_local_gate_of_objspace(objspace), GET_RACTOR()); }

#define SUSPEND_OBJSPACE_LOCK_BEGIN(objspace) { \
    bool objspace_lock_suspended = false; \
    int _old_objspace_lock_level; \
    rb_objspace_gate_t *_local_gate = rb_gc_local_gate_of_objspace(objspace); \
    if (_local_gate->objspace_lock_owner == GET_RACTOR()) { \
	objspace_lock_suspended = true; \
	_old_objspace_lock_level = _local_gate->objspace_lock_level; \
	_local_gate->objspace_lock_owner = NULL; \
	_local_gate->objspace_lock_level = 0; \
	rb_native_mutex_unlock(&_local_gate->objspace_lock); \
    }

#define SUSPEND_OBJSPACE_LOCK_END(objspace) \
    if (objspace_lock_suspended) { \
	rb_native_mutex_lock(&_local_gate->objspace_lock); \
	_local_gate->objspace_lock_owner = GET_RACTOR(); \
	_local_gate->objspace_lock_level = _old_objspace_lock_level; \
    } \
}

void local_gc_running_on(rb_objspace_gate_t *local_gate);
void local_gc_running_off(rb_objspace_gate_t *local_gate);

void begin_local_gc_section(rb_objspace_gate_t *local_gate, rb_ractor_t *cr);
void end_local_gc_section(rb_objspace_gate_t *local_gate, rb_ractor_t *cr);
void begin_global_gc_section(rb_objspace_coordinator_t *coordinator, rb_objspace_gate_t *local_gate, unsigned int *lev);
void end_global_gc_section(rb_objspace_coordinator_t *coordinator, rb_objspace_gate_t *local_gate, unsigned int *lev);

#define LOCAL_GC_BEGIN(objspace) \
{ \
    SUSPEND_OBJSPACE_LOCK_BEGIN(objspace); \
    begin_local_gc_section(rb_gc_local_gate_of_objspace(objspace), GET_RACTOR());

#define LOCAL_GC_END(objspace) \
    end_local_gc_section(rb_gc_local_gate_of_objspace(objspace), GET_RACTOR()); \
    SUSPEND_OBJSPACE_LOCK_END(objspace); \
}

#define GLOBAL_GC_BEGIN(coordinator, objspace) \
{ \
    SUSPEND_OBJSPACE_LOCK_BEGIN(objspace); \
    unsigned int _lev; \
    begin_global_gc_section(coordinator, rb_gc_local_gate_of_objspace(objspace), &_lev); \


#define GLOBAL_GC_END(coordinator, objspace) \
    end_global_gc_section(coordinator, rb_gc_local_gate_of_objspace(objspace), &_lev); \
    SUSPEND_OBJSPACE_LOCK_END(objspace); \
}

bool gc_deactivated(rb_objspace_coordinator_t *coordinator);
VALUE gc_deactivate_no_rest(rb_objspace_coordinator_t *coordinator);
VALUE rb_gc_deactivate(rb_objspace_coordinator_t *coordinator);

#if VM_CHECK_MODE > 0
bool rb_ractor_safe_gc_state(void);
#endif

#define ASSERT_ractor_safe_gc_state() VM_ASSERT(rb_ractor_safe_gc_state())

bool global_gc_needed(void);
void arrange_next_gc_global_status(double sharedobject_limit_factor);
void global_gc_for_each_objspace(rb_vm_t *vm, rb_objspace_gate_t *runner_gate, void (gc_func)(struct rb_objspace *objspace));
void rb_objspace_call_finalizer_for_each_ractor(rb_vm_t *vm);
void rb_gc_writebarrier_multi_objspace(VALUE a, VALUE b);

#define WITH_MARK_FUNC_BEGIN(_mark_func, _data_ptr) do { \
    struct gc_mark_func_data_struct _mfd = { \
	.mark_func = _mark_func, \
	.data = _data_ptr, \
    }; \
    struct gc_mark_func_data_struct *prev_mark_func_data = GET_RACTOR()->local_gate->mark_func_data; \
    GET_RACTOR()->local_gate->mark_func_data = (&_mfd);

#define WITH_MARK_FUNC_END() GET_RACTOR()->local_gate->mark_func_data = prev_mark_func_data;} while (0)

#define MARK_FUNC_IN_USE(cr) (cr->local_gate->mark_func_data != NULL)

#define MARK_FUNC_RUN(cr, obj) do { \
    VM_ASSERT(MARK_FUNC_IN_USE(cr)); \
    struct gc_mark_func_data_struct *mark_func_data = cr->local_gate->mark_func_data; \
    cr->local_gate->mark_func_data = NULL; \
    mark_func_data->mark_func((obj), mark_func_data->data); \
    cr->local_gate->mark_func_data = mark_func_data; \
} while (0)

#endif
