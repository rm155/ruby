#include "glospace.h"
#include "internal/gc.h"
#include "internal/thread.h"
#include "vm_core.h"
#include "vm_sync.h"
#include "ractor_core.h"
#include "vm_debug.h"

void rb_ractor_sched_barrier_start(rb_vm_t *vm, rb_ractor_t *cr);
void rb_ractor_sched_barrier_join(rb_vm_t *vm, rb_ractor_t *cr);

static bool
vm_locked(rb_vm_t *vm)
{
    return vm->ractor.sync.lock_owner == GET_RACTOR();
}

#if RUBY_DEBUG > 0
void
RUBY_ASSERT_vm_locking(void)
{
    if (rb_multi_ractor_p()) {
        rb_vm_t *vm = GET_VM();
        VM_ASSERT(vm_locked(vm));
    }
}

void
RUBY_ASSERT_vm_unlocking(void)
{
    if (rb_multi_ractor_p()) {
        rb_vm_t *vm = GET_VM();
        VM_ASSERT(!vm_locked(vm));
    }
}
#endif

bool
rb_vm_locked_p(void)
{
    return vm_locked(GET_VM());
}

static bool
vm_need_barrier_waiting(const rb_vm_t *vm)
{
#ifdef RUBY_THREAD_PTHREAD_H
    return vm->ractor.sched.barrier_waiting;
#else
    return vm->ractor.sync.barrier_waiting;
#endif
}

static bool
vm_need_barrier(bool no_barrier, const rb_ractor_t *cr, const rb_vm_t *vm)
{
#ifdef RUBY_THREAD_PTHREAD_H
    return !no_barrier && cr->threads.sched.running != NULL && vm_need_barrier_waiting(vm); // ractor has running threads.
#else
    return !no_barrier && vm_need_barrier_waiting(vm);
#endif
}

static bool
can_use_barrier(rb_ractor_t *cr)
{
    return !cr || cr->teardown_cleanup_done || cr->late_to_barrier || !cr->local_objspace || !rb_during_local_gc();
}

static void
vm_lock_enter(rb_ractor_t *cr, rb_vm_t *vm, bool locked, bool no_barrier, unsigned int *lev APPEND_LOCATION_ARGS)
{
    RUBY_DEBUG_LOG2(file, line, "start locked:%d", locked);

    no_barrier = no_barrier || !can_use_barrier(cr);
    VM_ASSERT(!cr || !cr->local_objspace || !heap_locked(cr->local_objspace));

    if (locked) {
        ASSERT_vm_locking();
    }
    else {
	if (!no_barrier) rb_ractor_object_graph_safety_advance(cr, OGS_FLAG_ENTERING_VM_LOCK);

#if RACTOR_CHECK_MODE
        // locking ractor and acquire VM lock will cause deadlock
        VM_ASSERT(cr->sync.locked_by != rb_ractor_self(cr) || cr->sync.locking_thread != GET_THREAD());
#endif

	rb_borrowing_status_pause(cr);

        // lock
        rb_native_mutex_lock(&vm->ractor.sync.lock);
        VM_ASSERT(vm->ractor.sync.lock_owner == NULL);
        VM_ASSERT(vm->ractor.sync.lock_rec == 0);

	rb_borrowing_status_resume(cr);
        // barrier
        if (vm_need_barrier(no_barrier, cr, vm)) {
            rb_execution_context_t *ec = GET_EC();
            RB_VM_SAVE_MACHINE_CONTEXT(rb_ec_thread_ptr(ec));

            do {
                VM_ASSERT(vm_need_barrier_waiting(vm));
                RUBY_DEBUG_LOG("barrier serial:%u", vm->ractor.sched.barrier_serial);
                rb_ractor_sched_barrier_join(vm, cr);
            } while (vm_need_barrier_waiting(vm));
        }

        VM_ASSERT(vm->ractor.sync.lock_rec == 0);
        VM_ASSERT(vm->ractor.sync.lock_owner == NULL);
        vm->ractor.sync.lock_owner = cr;

	if (!no_barrier) rb_ractor_object_graph_safety_withdraw(cr, OGS_FLAG_ENTERING_VM_LOCK);
    }

    vm->ractor.sync.lock_rec++;
    *lev = vm->ractor.sync.lock_rec;

    RUBY_DEBUG_LOG2(file, line, "rec:%u owner:%u", vm->ractor.sync.lock_rec,
                    (unsigned int)rb_ractor_id(vm->ractor.sync.lock_owner));
}

static void
vm_lock_leave(rb_vm_t *vm, unsigned int *lev APPEND_LOCATION_ARGS)
{
    RUBY_DEBUG_LOG2(file, line, "rec:%u owner:%u%s", vm->ractor.sync.lock_rec,
                    (unsigned int)rb_ractor_id(vm->ractor.sync.lock_owner),
                    vm->ractor.sync.lock_rec == 1 ? " (leave)" : "");

    ASSERT_vm_locking();
    VM_ASSERT(vm->ractor.sync.lock_rec > 0);
    VM_ASSERT(vm->ractor.sync.lock_rec == *lev);

    vm->ractor.sync.lock_rec--;
    *lev = vm->ractor.sync.lock_rec;

    if (vm->ractor.sync.lock_rec == 0) {
        vm->ractor.sync.lock_owner = NULL;
        rb_native_mutex_unlock(&vm->ractor.sync.lock);
    }
}

void
rb_vm_lock_enter_body(unsigned int *lev APPEND_LOCATION_ARGS)
{
    rb_vm_t *vm = GET_VM();
    if (vm_locked(vm)) {
        vm_lock_enter(NULL, vm, true, false, lev APPEND_LOCATION_PARAMS);
    }
    else {
        vm_lock_enter(GET_RACTOR(), vm, false, false, lev APPEND_LOCATION_PARAMS);
    }
}

void
rb_vm_lock_enter_body_nb(unsigned int *lev APPEND_LOCATION_ARGS)
{
    rb_vm_t *vm = GET_VM();
    if (vm_locked(vm)) {
        vm_lock_enter(NULL, vm, true, true, lev APPEND_LOCATION_PARAMS);
    }
    else {
        vm_lock_enter(GET_RACTOR(), vm, false, true, lev APPEND_LOCATION_PARAMS);
    }
}

void
rb_vm_lock_enter_body_cr(rb_ractor_t *cr, unsigned int *lev APPEND_LOCATION_ARGS)
{
    rb_vm_t *vm = GET_VM();
    vm_lock_enter(cr, vm, vm_locked(vm), false, lev APPEND_LOCATION_PARAMS);
}

void
rb_vm_lock_leave_body(unsigned int *lev APPEND_LOCATION_ARGS)
{
    vm_lock_leave(GET_VM(), lev APPEND_LOCATION_PARAMS);
}

void
rb_vm_lock_body(LOCATION_ARGS)
{
    rb_vm_t *vm = GET_VM();
    ASSERT_vm_unlocking();

    vm_lock_enter(GET_RACTOR(), vm, false, false, &vm->ractor.sync.lock_rec APPEND_LOCATION_PARAMS);
}

void
rb_vm_unlock_body(LOCATION_ARGS)
{
    rb_vm_t *vm = GET_VM();
    ASSERT_vm_locking();
    VM_ASSERT(vm->ractor.sync.lock_rec == 1);
    vm_lock_leave(vm, &vm->ractor.sync.lock_rec APPEND_LOCATION_PARAMS);
}

static void
vm_cond_wait(rb_vm_t *vm, rb_nativethread_cond_t *cond, unsigned long msec)
{
    ASSERT_vm_locking();
    unsigned int lock_rec = vm->ractor.sync.lock_rec;
    rb_ractor_t *cr = vm->ractor.sync.lock_owner;

    vm->ractor.sync.lock_rec = 0;
    vm->ractor.sync.lock_owner = NULL;
    if (msec > 0) {
        rb_native_cond_timedwait(cond, &vm->ractor.sync.lock, msec);
    }
    else {
        rb_native_cond_wait(cond, &vm->ractor.sync.lock);
    }
    vm->ractor.sync.lock_rec = lock_rec;
    vm->ractor.sync.lock_owner = cr;
}

void
rb_vm_cond_wait(rb_vm_t *vm, rb_nativethread_cond_t *cond)
{
    vm_cond_wait(vm, cond, 0);
}

void
rb_vm_cond_timedwait(rb_vm_t *vm, rb_nativethread_cond_t *cond, unsigned long msec)
{
    vm_cond_wait(vm, cond, msec);
}

void
rb_vm_barrier(void)
{
    RB_DEBUG_COUNTER_INC(vm_sync_barrier);

    if (!rb_multi_ractor_p()) {
        // no other ractors
        return;
    }
    else {
        rb_vm_t *vm = GET_VM();
        VM_ASSERT(!vm->ractor.sched.barrier_waiting);
        ASSERT_vm_locking();
        rb_ractor_t *cr = vm->ractor.sync.lock_owner;
        VM_ASSERT(cr == GET_RACTOR());
        VM_ASSERT(rb_ractor_status_p(cr, ractor_running));

	rb_ractor_object_graph_safety_advance(cr, OGS_FLAG_BARRIER_CREATING);
        rb_ractor_sched_barrier_start(vm, cr);
	rb_ractor_object_graph_safety_withdraw(cr, OGS_FLAG_BARRIER_CREATING);
    }
}

void
rb_ec_vm_lock_rec_release(const rb_execution_context_t *ec,
                          unsigned int recorded_lock_rec,
                          unsigned int current_lock_rec)
{
    VM_ASSERT(recorded_lock_rec != current_lock_rec);

    if (UNLIKELY(recorded_lock_rec > current_lock_rec)) {
        rb_bug("unexpected situation - recordd:%u current:%u",
               recorded_lock_rec, current_lock_rec);
    }
    else {
        while (recorded_lock_rec < current_lock_rec) {
            RB_VM_LOCK_LEAVE_LEV(&current_lock_rec);
        }
    }

    VM_ASSERT(recorded_lock_rec == rb_ec_vm_lock_rec(ec));
}
