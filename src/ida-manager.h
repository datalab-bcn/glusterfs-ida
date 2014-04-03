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

#define IDA_SKIP_DFC ((dfc_transaction_t *)0)
#define IDA_USE_DFC ((dfc_transaction_t *)1)

#define IDA_IS_TXN(_txn) ((uintptr_t)(_txn) > 1)

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
    sys_mutex_t lock;
    xlator_t ** xl_list;
    dfc_t *     dfc;
    uintptr_t * delay;
    int32_t     index;
    bool        up;
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
    bool           (* prepare)(ida_private_t *, ida_request_t *);
    void           (* dispatch)(ida_private_t *, ida_request_t *);
    void           (* completed)(call_frame_t *, err_t, ida_request_t *,
                                 uintptr_t *);
    bool           (* combine)(ida_request_t *, uint32_t, ida_answer_t *,
                               uintptr_t *);
    int32_t        (* rebuild)(ida_private_t *, ida_request_t *,
                               ida_answer_t *);
    ida_answer_t * (* copy)(uintptr_t *);
};

struct _ida_request
{
    call_frame_t *      frame;
    call_frame_t *      rframe;
    xlator_t *          xl;
    ida_handlers_t *    handlers;
    dfc_transaction_t * txn;
    loc_t               loc1;
    loc_t               loc2;
    fd_t *              fd;
    int32_t             minimum;
    int32_t             required;
    int32_t             pending;
    size_t              size;
    uintptr_t           flags;
    uintptr_t           data;
    uintptr_t           sent;
    uintptr_t           last_sent;
    uintptr_t           failed;
    uintptr_t           bad;
    dict_t **           xdata;
    sys_lock_t          lock;
    struct list_head    answers;
    int32_t             completed;
//    int32_t             dfc;
};

struct _ida_answer
{
    struct list_head list;
    uint32_t         count;
    int32_t          id;
    uintptr_t        mask;
    ida_answer_t *   next;
};

#define IDA_REQ_SIZE SYS_CALLS_ADJUST_SIZE(sizeof(ida_request_t))
#define IDA_ANS_SIZE SYS_CALLS_ADJUST_SIZE(sizeof(ida_answer_t))

#define IDA_FOP_DECLARE(_fop) \
    SYS_ASYNC_DECLARE(ida_##_fop, ((call_frame_t *, frame), \
                                   (xlator_t *, this), \
                                   (ida_handlers_t *, handlers), \
                                   (dfc_transaction_t *, txn), \
                                   (uintptr_t, bad), \
                                   (int32_t, minimum), \
                                   (int32_t, required), \
                                   (loc_t, loc1, PTR, sys_loc_acquire, \
                                                      sys_loc_release), \
                                   (loc_t, loc2, PTR, sys_loc_acquire, \
                                                      sys_loc_release), \
                                   (fd_t *, fd1, COPY, sys_fd_acquire, \
                                                       sys_fd_release), \
                                   SYS_GF_ARGS_##_fop)) \
    void ida_completed_##_fop(call_frame_t * frame, err_t error, \
                              ida_request_t * req, uintptr_t * data); \
    ida_answer_t * ida_copy_##_fop(uintptr_t * io); \
    extern ida_handlers_t ida_handlers_##_fop

IDA_FOP_DECLARE(access);
IDA_FOP_DECLARE(create);
IDA_FOP_DECLARE(entrylk);
IDA_FOP_DECLARE(fentrylk);
IDA_FOP_DECLARE(flush);
IDA_FOP_DECLARE(fsync);
IDA_FOP_DECLARE(fsyncdir);
IDA_FOP_DECLARE(getxattr);
IDA_FOP_DECLARE(fgetxattr);
IDA_FOP_DECLARE(inodelk);
IDA_FOP_DECLARE(finodelk);
IDA_FOP_DECLARE(link);
IDA_FOP_DECLARE(lk);
IDA_FOP_DECLARE(lookup);
IDA_FOP_DECLARE(mkdir);
IDA_FOP_DECLARE(mknod);
IDA_FOP_DECLARE(open);
IDA_FOP_DECLARE(opendir);
IDA_FOP_DECLARE(rchecksum);
IDA_FOP_DECLARE(readdir);
IDA_FOP_DECLARE(readdirp);
IDA_FOP_DECLARE(readlink);
IDA_FOP_DECLARE(readv);
IDA_FOP_DECLARE(removexattr);
IDA_FOP_DECLARE(fremovexattr);
IDA_FOP_DECLARE(rename);
IDA_FOP_DECLARE(rmdir);
IDA_FOP_DECLARE(setattr);
IDA_FOP_DECLARE(fsetattr);
IDA_FOP_DECLARE(setxattr);
IDA_FOP_DECLARE(fsetxattr);
IDA_FOP_DECLARE(stat);
IDA_FOP_DECLARE(fstat);
IDA_FOP_DECLARE(statfs);
IDA_FOP_DECLARE(symlink);
IDA_FOP_DECLARE(truncate);
IDA_FOP_DECLARE(ftruncate);
IDA_FOP_DECLARE(unlink);
IDA_FOP_DECLARE(writev);
IDA_FOP_DECLARE(xattrop);
IDA_FOP_DECLARE(fxattrop);

void ida_dispatch_incremental(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_all(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_minimum(ida_private_t * ida, ida_request_t * req);
void ida_dispatch_write(ida_private_t * ida, ida_request_t * req);

#endif /* __IDA_MANAGER_H__ */
