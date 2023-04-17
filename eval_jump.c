/* -*-c-*- */
/*
 * from eval.c
 */

#include "eval_intern.h"
#include "internal/gc.h"

/* exit */

void
rb_call_end_proc(VALUE data)
{
    rb_proc_call(data, rb_ary_new());
}

/*
 *  call-seq:
 *     at_exit { block } -> proc
 *
 *  Converts _block_ to a +Proc+ object (and therefore
 *  binds it at the point of call) and registers it for execution when
 *  the program exits. If multiple handlers are registered, they are
 *  executed in reverse order of registration.
 *
 *     def do_at_exit(str1)
 *       at_exit { print str1 }
 *     end
 *     at_exit { puts "cruel world" }
 *     do_at_exit("goodbye ")
 *     exit
 *
 *  <em>produces:</em>
 *
 *     goodbye cruel world
 */

static VALUE
rb_f_at_exit(VALUE _)
{
    VALUE proc;

    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "called without a block");
    }
    proc = rb_block_proc();
    rb_set_end_proc(rb_call_end_proc, proc);
    return proc;
}

struct end_proc_data {
    void (*func) (VALUE);
    VALUE data;
    struct end_proc_data *next;
    rb_objspace_link_t *objspace_link;
};

static struct end_proc_data *end_procs, *ephemeral_end_procs;

static void
regular_end_procs_foreach(void (*func) (struct end_proc_data *, void *), void *data)
{
    struct end_proc_data *link;

    link = end_procs;
    while (link) {
        func(link, data);
        link = link->next;
    }
}

static void
ephemeral_end_procs_foreach(void (*func) (struct end_proc_data *, void *), void *data)
{
    struct end_proc_data *link;

    link = ephemeral_end_procs;
    while (link) {
        func(link, data);
        link = link->next;
    }
}

static void
end_procs_foreach(void (*func) (struct end_proc_data *, void *), void *data)
{
    regular_end_procs_foreach(func, data);
    ephemeral_end_procs_foreach(func, data);
}

void
update_end_proc_objspace_link(struct end_proc_data *end_proc, void *dmy)
{
    if (end_proc->objspace_link->link_changed) {
	end_proc->objspace_link = get_updated_objspace_link(end_proc->objspace_link);
    }
}

void
rb_update_all_end_proc_objspace_links(void)
{
    end_procs_foreach(update_end_proc_objspace_link, NULL);
}

void
rb_set_end_proc(void (*func)(VALUE), VALUE data)
{
    struct end_proc_data *link = ALLOC(struct end_proc_data);
    struct end_proc_data **list;
    rb_thread_t *th = GET_THREAD();

    if (th->top_wrapper) {
        list = &ephemeral_end_procs;
    }
    else {
        list = &end_procs;
    }
    link->next = *list;
    link->func = func;
    link->data = data;
    link->objspace_link = get_objspace_link_of_value(data);
    *list = link;
}

static void
mark_end_proc_if_in_objspace(struct end_proc_data *end_proc, void *arg)
{
    struct rb_objspace *objspace = arg;
    if (objspace == end_proc->objspace_link->linked_objspace) rb_gc_mark(end_proc->data);
}

void
rb_mark_end_proc(struct rb_objspace *objspace)
{
    end_procs_foreach(mark_end_proc_if_in_objspace, objspace);
}

static void
exec_end_procs_chain(struct end_proc_data *volatile *procs, VALUE *errp)
{
    struct end_proc_data volatile endproc;
    struct end_proc_data *link;
    VALUE errinfo = *errp;

    while ((link = *procs) != 0) {
        *procs = link->next;
        endproc = *link;
        xfree(link);
        (*endproc.func) (endproc.data);
        *errp = errinfo;
    }
}

static void
rb_ec_exec_end_proc(rb_execution_context_t * ec)
{
    enum ruby_tag_type state;
    volatile VALUE errinfo = ec->errinfo;

    EC_PUSH_TAG(ec);
    if ((state = EC_EXEC_TAG()) == TAG_NONE) {
      again:
        exec_end_procs_chain(&ephemeral_end_procs, &ec->errinfo);
        exec_end_procs_chain(&end_procs, &ec->errinfo);
    }
    else {
        EC_TMPPOP_TAG();
        error_handle(ec, ec->errinfo, state);
        if (!NIL_P(ec->errinfo)) errinfo = ec->errinfo;
        EC_REPUSH_TAG();
        goto again;
    }
    EC_POP_TAG();

    ec->errinfo = errinfo;
}

void
Init_jump(void)
{
    rb_define_global_function("at_exit", rb_f_at_exit, 0);
}
