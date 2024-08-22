#ifndef RUBY_RACTOR_CORE_H
#define RUBY_RACTOR_CORE_H

#include "internal/gc.h"
#include "ruby/ruby.h"
#include "ruby/ractor.h"
#include "vm_core.h"
#include "id_table.h"
#include "vm_debug.h"


#ifndef RACTOR_CHECK_MODE
#define RACTOR_CHECK_MODE (VM_CHECK_MODE || RUBY_DEBUG) && (SIZEOF_UINT64_T == SIZEOF_VALUE)
#endif

enum rb_ractor_basket_type {
    // basket is empty
    basket_type_none,

    // value is available
    basket_type_ref,
    basket_type_copy,
    basket_type_move,
    basket_type_will,

    // basket should be deleted
    basket_type_deleted,

    // basket is reserved
    basket_type_reserved,

    // take_basket is available
    basket_type_take_basket,

    // basket is keeping by yielding ractor
    basket_type_yielding,
};

// per ractor taking configuration
struct rb_ractor_selector_take_config {
    bool closed;
    bool oneshot;
};

struct rb_ractor_basket {
    union {
        enum rb_ractor_basket_type e;
        rb_atomic_t atomic;
    } type;
    VALUE sender;

    union {
        struct {
            VALUE v;
            bool exception;
        } send;

        struct {
            struct rb_ractor_basket *basket;
            struct rb_ractor_selector_take_config *config;
        } take;
    } p; // payload
};

static inline bool
basket_type_p(struct rb_ractor_basket *b, enum rb_ractor_basket_type type)
{
    return b->type.e == type;
}

static inline bool
basket_none_p(struct rb_ractor_basket *b)
{
    return basket_type_p(b, basket_type_none);
}

struct rb_ractor_queue {
    struct rb_ractor_basket *baskets;
    int start;
    int cnt;
    int size;
    unsigned int serial;
    unsigned int reserved_cnt;
};

enum rb_ractor_wait_status {
    wait_none      = 0x00,
    wait_receiving = 0x01,
    wait_taking    = 0x02,
    wait_yielding  = 0x04,
    wait_moving    = 0x08,
};

enum rb_ractor_wakeup_status {
    wakeup_none,
    wakeup_by_send,
    wakeup_by_yield,
    wakeup_by_take,
    wakeup_by_close,
    wakeup_by_interrupt,
    wakeup_by_retry,
};

struct rb_ractor_sync {
    // ractor lock
    rb_nativethread_lock_t lock;

    rb_nativethread_cond_t close_cond;
    bool ready_to_close;

    VALUE locked_by;
    VALUE locking_thread;

    bool incoming_port_closed;
    bool outgoing_port_closed;

    // All sent messages will be pushed into recv_queue
    struct rb_ractor_queue recv_queue;

    // The following ractors waiting for the yielding by this ractor
    struct rb_ractor_queue takers_queue;

    // Enabled if the ractor already terminated and not taken yet.
    struct rb_ractor_basket will_basket;

    struct ractor_wait {
        enum rb_ractor_wait_status status;
        enum rb_ractor_wakeup_status wakeup_status;
        rb_thread_t *waiting_thread;
    } wait;

#ifndef RUBY_THREAD_PTHREAD_H
    rb_nativethread_cond_t cond;
#endif
};

// created
//   | ready to run
// ====================== inserted to vm->ractor
//   v
// blocking <---+ all threads are blocking
//   |          |
//   v          |
// running -----+
//   | all threads are terminated.
// ====================== removed from vm->ractor
//   v
// terminated
//
// status is protected by VM lock (global state)
enum ractor_status {
    ractor_created,
    ractor_running,
    ractor_blocking,
    ractor_terminated,
};

struct rb_global_space;
struct rb_objspace;
typedef struct rb_objspace_gate rb_objspace_gate_t;

struct borrowing_target_node_t {
    rb_ractor_t *target_ractor;
    struct borrowing_target *next;
};

struct global_object_list {
    VALUE *varptr;
    struct global_object_list *next;
};

#define RACTOR_CACHE_COUNT 2

struct rb_ractor_struct {
    struct rb_ractor_pub pub;

    struct rb_ractor_sync sync;
    VALUE receiving_mutex;
    bool during_teardown_cleanup;
    bool teardown_cleanup_done;
#if VM_CHECK_MODE > 0
    bool reached_insertion;
#endif
    rb_ractor_t *receiver_before_exit;

    // vm wide barrier synchronization
    rb_nativethread_cond_t barrier_wait_cond;

    // thread management
    struct {
        struct ccan_list_head set;
        unsigned int cnt;
        unsigned int blocking_cnt;
        unsigned int sleeper;
        struct rb_thread_sched sched;
        rb_execution_context_t *running_ec;
        rb_thread_t *main;
    } threads;
    VALUE thgroup_default;
    rb_thread_t *dropped_main_thread;

    VALUE name;
    VALUE loc;

    enum ractor_status status_;

    struct ccan_list_node vmlr_node;
    struct ccan_list_node objspace_node;

    // ractor local data

    st_table *local_storage;
    struct rb_id_table *idkey_local_storage;

    VALUE r_stdin;
    VALUE r_stdout;
    VALUE r_stderr;
    VALUE verbose;
    VALUE debug;

    void *newobj_cache;
    void *newobj_borrowing_cache;
    struct {
	rb_nativethread_lock_t lock;
	rb_ractor_t *lock_owner;
	int lock_lev;

	rb_atomic_t borrower_count;
	rb_nativethread_cond_t no_borrowers;

	bool borrowing_closed;
	bool borrowing_allowed;
	rb_nativethread_cond_t borrowing_allowed_cond;
	rb_nativethread_lock_t borrowing_allowed_lock;

	uintptr_t borrowing_id;
    } borrowing_sync;

    // gc.c rb_objspace_reachable_objects_from
    struct gc_mark_func_data_struct {
        void *data;
        void (*mark_func)(VALUE v, void *data);
	bool global;
    } *mfd;

    struct rb_objspace *local_objspace;
    rb_objspace_gate_t *local_gate;

    bool late_to_barrier;
#if VM_CHECK_MODE > 0
    bool during_ractor_copy;
#endif

    VALUE mark_object_ary;
    rb_nativethread_lock_t mark_object_ary_lock;

    struct global_object_list *global_object_list;

    bool registered_in_ractor_chains;
    struct rb_ractor_chain_node *ogs_chain_node;
    struct rb_ractor_chain_node *gc_chain_node;

    VALUE result_value;

    bool objspace_absorbed;

    struct borrowing_target_node_t *borrowing_target_top;
}; // rb_ractor_t is defined in vm_core.h

static inline VALUE
rb_ractor_self(const rb_ractor_t *r)
{
    return r->pub.self;
}

rb_ractor_t *rb_ractor_main_alloc(void);
void rb_ractor_main_setup(rb_vm_t *vm, rb_ractor_t *main_ractor, rb_thread_t *main_thread);
void rb_ractor_atexit(rb_execution_context_t *ec, VALUE result);
void rb_ractor_atexit_exception(rb_execution_context_t *ec);
void rb_ractor_teardown(rb_execution_context_t *ec);
void rb_ractor_receive_parameters(rb_execution_context_t *ec, rb_ractor_t *g, int len, VALUE *ptr);
void rb_ractor_send_parameters(rb_execution_context_t *ec, rb_ractor_t *g, VALUE args);

void rb_thread_create_ractor(rb_ractor_t *g, VALUE args, VALUE proc); // defined in thread.c

int rb_ractor_living_thread_num(const rb_ractor_t *);
VALUE rb_ractor_thread_list(void);
bool rb_ractor_p(VALUE rv);

void rb_ractor_living_threads_init(rb_ractor_t *r);
void rb_ractor_living_threads_insert(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_living_threads_remove(rb_ractor_t *r, rb_thread_t *th);
void rb_ractor_blocking_threads_inc(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG
void rb_ractor_blocking_threads_dec(rb_ractor_t *r, const char *file, int line); // TODO: file, line only for RUBY_DEBUG_LOG

void lock_ractor_set(void);
void unlock_ractor_set(void);

void rb_ractor_chains_register(rb_ractor_t *r);
void rb_ractor_chains_unregister(rb_ractor_t *r);

void rb_borrowing_sync_lock(rb_ractor_t *r);
void rb_borrowing_sync_unlock(rb_ractor_t *r);
void rb_ractor_borrowing_barrier_begin(rb_ractor_t *r);
void rb_ractor_borrowing_barrier_end(rb_ractor_t *r);

void rb_borrowing_status_pause(rb_ractor_t *cr);
void rb_borrowing_status_resume(rb_ractor_t *cr);

void rb_ractor_vm_barrier_interrupt_running_thread(rb_ractor_t *r);
void rb_ractor_terminate_interrupt_main_thread(rb_ractor_t *r);
void rb_ractor_terminate_all(void);
bool rb_ractor_main_p_(void);
void rb_ractor_mark_object_ary_init(rb_ractor_t *r);
void rb_ractor_related_objects_mark(void *ptr);
void rb_ractor_update_references(void *ptr);
void rb_ractor_atfork(rb_vm_t *vm, rb_thread_t *th);

VALUE rb_ractor_ensure_shareable(VALUE obj, VALUE name);

RUBY_SYMBOL_EXPORT_BEGIN
void rb_ractor_finish_marking(void);

bool rb_ractor_shareable_p_continue(VALUE obj);

// THIS FUNCTION SHOULD NOT CALL WHILE INCREMENTAL MARKING!!
// This function is for T_DATA::free_func
void rb_ractor_local_storage_delkey(rb_ractor_local_key_t key);

RUBY_SYMBOL_EXPORT_END

static inline bool
rb_ractor_main_p(void)
{
    if (ruby_single_main_ractor) {
        return true;
    }
    else {
        return rb_ractor_main_p_();
    }
}

static inline bool
rb_ractor_status_p(rb_ractor_t *r, enum ractor_status status)
{
    return r->status_ == status;
}

static inline void
rb_ractor_sleeper_threads_inc(rb_ractor_t *r)
{
    r->threads.sleeper++;
}

static inline void
rb_ractor_sleeper_threads_dec(rb_ractor_t *r)
{
    r->threads.sleeper--;
}

static inline void
rb_ractor_sleeper_threads_clear(rb_ractor_t *r)
{
    r->threads.sleeper = 0;
}

static inline int
rb_ractor_sleeper_thread_num(rb_ractor_t *r)
{
    return r->threads.sleeper;
}

static inline void
rb_ractor_thread_switch(rb_ractor_t *cr, rb_thread_t *th)
{
    RUBY_DEBUG_LOG("th:%d->%u%s",
                   cr->threads.running_ec ? (int)rb_th_serial(cr->threads.running_ec->thread_ptr) : -1,
                   rb_th_serial(th), cr->threads.running_ec == th->ec ? " (same)" : "");

    if (cr->threads.running_ec != th->ec) {
        if (0) {
            ruby_debug_printf("rb_ractor_thread_switch ec:%p->%p\n",
                              (void *)cr->threads.running_ec, (void *)th->ec);
        }
    }
    else {
        return;
    }

    if (cr->threads.running_ec != th->ec) {
        th->running_time_us = 0;
    }

    cr->threads.running_ec = th->ec;

    VM_ASSERT(cr == GET_RACTOR());
}

static inline void
rb_ractor_set_current_ec_no_ractor(rb_execution_context_t *ec)
{
#ifdef RB_THREAD_LOCAL_SPECIFIER

# ifdef __APPLE__
    rb_current_ec_set(ec);
# else
    ruby_current_ec = ec;
# endif

#else
    native_tls_set(ruby_current_ec_key, ec);
#endif
}

static inline void
rb_ractor_set_current_os_gate(rb_objspace_gate_t *os_gate)
{
#ifdef RB_THREAD_LOCAL_SPECIFIER

# ifdef __APPLE__
    rb_current_os_gate_set(os_gate);
# else
    ruby_current_os_gate = os_gate;
# endif

#else
    native_tls_set(ruby_current_os_gate_key, os_gate);
#endif
}

#define rb_ractor_set_current_ec(cr, ec) rb_ractor_set_current_ec_(cr, ec, __FILE__, __LINE__)

static inline void
rb_ractor_set_current_ec_(rb_ractor_t *cr, rb_execution_context_t *ec, const char *file, int line)
{
    rb_ractor_set_current_ec_no_ractor(ec);
    rb_ractor_set_current_os_gate(cr->local_gate);
    RUBY_DEBUG_LOG2(file, line, "ec:%p->%p", (void *)cr->threads.running_ec, (void *)ec);
    VM_ASSERT(ec == NULL || cr->threads.running_ec != ec);
    cr->threads.running_ec = ec;
}

void rb_vm_ractor_blocking_cnt_inc(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);
void rb_vm_ractor_blocking_cnt_dec(rb_vm_t *vm, rb_ractor_t *cr, const char *file, int line);

static inline uint32_t
rb_ractor_id(const rb_ractor_t *r)
{
    return r->pub.id;
}

#if RACTOR_CHECK_MODE > 0
# define RACTOR_BELONGING_ID(obj) (*(uint32_t *)(((uintptr_t)(obj)) + rb_gc_obj_slot_size(obj)))

uint32_t rb_ractor_current_id(void);

static inline void
rb_ractor_setup_belonging_to(VALUE obj, uint32_t rid)
{
    RACTOR_BELONGING_ID(obj) = rid;
}

static inline uint32_t
rb_ractor_belonging(VALUE obj)
{
    if (SPECIAL_CONST_P(obj) || RB_OBJ_SHAREABLE_P(obj)) {
        return 0;
    }
    else {
        return RACTOR_BELONGING_ID(obj);
    }
}

static inline VALUE
rb_ractor_confirm_belonging(VALUE obj)
{
    uint32_t id = rb_ractor_belonging(obj);

    if (id == 0) {
        if (UNLIKELY(!rb_ractor_shareable_p(obj))) {
            rp(obj);
            rb_bug("id == 0 but not shareable");
        }
    }
    else if (UNLIKELY(id != rb_ractor_current_id())) {
        if (rb_ractor_shareable_p(obj)) {
            // ok
        }
        else {
            rp(obj);
            rb_bug("rb_ractor_confirm_belonging object-ractor id:%u, current-ractor id:%u", id, rb_ractor_current_id());
        }
    }
    return obj;
}
#else
#define rb_ractor_confirm_belonging(obj) obj
#endif

#endif /* RUBY_RACTOR_CORE_H */
