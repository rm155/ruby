#ifndef RUBY_THREAD_NONE_H
#define RUBY_THREAD_NONE_H

#define RB_NATIVETHREAD_LOCK_INIT (void)(0)
#define RB_NATIVETHREAD_COND_INIT (void)(0)

// no-thread impl doesn't use TLS but define this to avoid using tls key
// based implementation in vm.c
#define RB_THREAD_LOCAL_SPECIFIER

struct rb_native_thread {
    void *thread_id; // NULL
};

struct rb_thread_sched_item {};
struct rb_thread_sched {};

RUBY_EXTERN struct rb_execution_context_struct *ruby_current_ec;
NOINLINE(struct rb_execution_context_struct *rb_current_ec_noinline(void)); // for assertions

RUBY_EXTERN rb_objspace_gate_t *ruby_current_os_gate;
NOINLINE(struct rb_objspace_gate *rb_current_os_gate_noinline(void)); // for assertions

#endif /* RUBY_THREAD_NONE_H */
