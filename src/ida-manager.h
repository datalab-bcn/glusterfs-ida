/*
  Copyright (c) 2012-2013 DataLab, S.L. <http://www.datalab.es>

  This file is part of the cluster/ida translator for GlusterFS.

  The cluster/ida translator for GlusterFS is free software: you can
  redistribute it and/or modify it under the terms of the GNU General
  Public License as published by the Free Software Foundation, either
  version 3 of the License, or (at your option) any later version.

  The cluster/ida translator for GlusterFS is distributed in the hope
  that it will be useful, but WITHOUT ANY WARRANTY; without even the
  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
  PURPOSE. See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with the cluster/ida translator for GlusterFS. If not, see
  <http://www.gnu.org/licenses/>.
*/

#ifndef __IDA_MANAGER_H__
#define __IDA_MANAGER_H__

#include "gfdfc.h"

#include "list.h"
#include "xlator.h"

#include "ida-types.h"

#define IDA_EXECUTE_MAX INT_MIN

typedef struct
{
    xlator_t *  xl;
    uint32_t    nodes;
    uint32_t    fragments;
    uint32_t    redundancy;
    uint32_t    block_size;
    uint64_t    device;
    uintptr_t   node_mask;
    uintptr_t   xl_up;
    uintptr_t   locked_mask;
    gf_lock_t   lock;
    xlator_t ** xl_list;
    dfc_t *     dfc;
} ida_private_t;

struct _ida_args_cbk
{
    struct list_head list;
    int32_t          result;
    int32_t          code;
    uint32_t         index;
    uint32_t         mixed_count;
    uintptr_t        mixed_mask;
    uint64_t         heal_available;
};

typedef struct
{
    char *                 name;
    ida_manager_wipe_f     wipe;
    ida_manager_dispatch_f dispatch;
    ida_manager_combine_f  combine;
    ida_manager_rebuild_f  rebuild;
    ida_manager_finish_f   finish;
} ida_manager_t;

typedef struct
{
    loc_t          loc;
    fd_t *         fd;
    int32_t        error;
    ida_callback_f callback;
    union
    {
        struct
        {
            off_t offset;
            off_t length;
        } inode;
        struct
        {
            const char * name;
        } entry;
    };
    struct
    {
        loc_t *        loc;
        fd_t *         fd;
        off_t          offset;
        off_t          length;
        ida_callback_f callback;
        ssize_t        size;
    } xattr;
} ida_flow_t;

struct _ida_local
{
    xlator_t * xl;
    ida_private_t * private;
    call_frame_t * frame;
    call_frame_t * next;
    int32_t refs;
    int32_t healing;
    int32_t pending;
    int32_t required;
    uintptr_t mask;
    ida_callback_f callback;
    ida_manager_t manager;
    int32_t error;
    uintptr_t completed_mask;
    uintptr_t succeeded_mask;
    int32_t   code;
    uint32_t  succeeded;
    uint32_t  reported;
    gf_lock_t lock;
    pid_t pid;
    gf_lkowner_t owner;
    ida_flow_t flow;
    struct list_head args_groups;
    inode_t * inode;
    ida_inode_ctx_t * inode_ctx;
};

struct _ida_request;
typedef struct _ida_request ida_request_t;

struct _ida_answer;
typedef struct _ida_answer ida_answer_t;

struct _ida_handlers;
typedef struct _ida_handlers ida_handlers_t;

struct _ida_handlers
{
    void           (* dispatch)(ida_private_t *, ida_request_t *);
    bool           (* combine)(ida_request_t *, uint32_t, ida_answer_t *,
                               uintptr_t *);
    int32_t        (* rebuild)(ida_private_t *, ida_request_t *,
                               ida_answer_t *);
    ida_answer_t * (* copy)(uintptr_t *);
};

struct _ida_request
{
    call_frame_t *      frame;
    xlator_t *          xl;
    ida_handlers_t *    handlers;
    dfc_transaction_t * txn;
    int32_t             required;
    int32_t             pending;
    size_t              size;
    uintptr_t           flags;
    uintptr_t           data;
    uintptr_t           sent;
    sys_lock_t          lock;
    struct list_head    answers;
    int32_t             completed;
};

struct _ida_answer
{
    struct list_head list;
    uint32_t         count;
    int32_t          id;
    ida_answer_t *   next;
};

#define IDA_REQ_SIZE SYS_CALLS_ADJUST_SIZE(sizeof(ida_request_t))
#define IDA_ANS_SIZE SYS_CALLS_ADJUST_SIZE(sizeof(ida_answer_t))

#define ida_mask_for_each(_index, _mask) \
    for ((_index) = (ffsll(_mask) - 1) & 127; (_index) < 64; (_index)++, (_index) += (ffsll((_mask) >> (_index)) - 1) & 127)

#define ida_args_cbk_for_each(_item, _local) \
    list_for_each(_item, &(_local)->args_groups)

#define ida_args_cbk_for_each_entry(_args, _local) \
    list_for_each_entry(_args, &(_local)->args_groups, list)

/*
int32_t ida_nest(ida_local_t ** local, xlator_t * xl, call_frame_t * base, inode_t * inode, ida_manager_t * manager, int32_t required, uintptr_t mask, ida_callback_f callback);
void ida_ref(ida_local_t * local);
void ida_unref(ida_local_t * local);
int32_t ida_dispatch(ida_local_t * local, int32_t index, ida_args_t * args);
void ida_execute(ida_local_t * local);
void ida_report(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args);
ida_local_t * ida_process(ida_args_cbk_t ** args, xlator_t * xl, call_frame_t * frame, void * cookie, int32_t result, int32_t code, dict_t * xdata);
void ida_combine(ida_local_t * local, ida_args_cbk_t * args, int32_t error);
void ida_complete(ida_local_t * local, int32_t error);
*/
#define IDA_WIND(_fop, _frame, _child, _index, _cbk, _args...) \
    STACK_WIND_COOKIE(_frame, _cbk, (void *)(uintptr_t)(_index), _child, (_child)->fops->_fop, ## _args)

#define IDA_UNWIND(_fop, _frame, _result, _code, _args...) \
    do \
    { \
        int32_t __tmp_code = _code; \
        if (unlikely((_result < 0) && (__tmp_code == 0))) \
        { \
            gf_log_callingfn("ida", GF_LOG_ERROR, "Detected error without an error code"); \
            __tmp_code = EIO; \
        } \
        STACK_UNWIND_STRICT(_fop, _frame, _result, __tmp_code, ## _args); \
    } while (0)

void ida_dispatch_incremental(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_all(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_minimum(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_write(ida_private_t * ida, ida_request_t * req);

#endif /* __IDA_MANAGER_H__ */
