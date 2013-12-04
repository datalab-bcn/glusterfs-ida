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

#include "list.h"
#include "xlator.h"

#include "ida-types.h"

#include "ida-fop-lookup.h"
#include "ida-fop-stat.h"
#include "ida-fop-truncate.h"
#include "ida-fop-access.h"
#include "ida-fop-readlink.h"
#include "ida-fop-mknod.h"
#include "ida-fop-mkdir.h"
#include "ida-fop-unlink.h"
#include "ida-fop-rmdir.h"
#include "ida-fop-symlink.h"
#include "ida-fop-rename.h"
#include "ida-fop-link.h"
#include "ida-fop-create.h"
#include "ida-fop-open.h"
#include "ida-fop-readv.h"
#include "ida-fop-writev.h"
#include "ida-fop-flush.h"
#include "ida-fop-fsync.h"
#include "ida-fop-opendir.h"
#include "ida-fop-readdir.h"
#include "ida-fop-statfs.h"
#include "ida-fop-xattr.h"
#include "ida-fop-lk.h"
#include "ida-fop-rchecksum.h"

#define IDA_EXECUTE_MAX INT_MIN

typedef struct
{
    uint32_t nodes;
    uint32_t fragments;
    uint32_t redundancy;
    uint32_t block_size;
    uint64_t device;
    uintptr_t node_mask;
    uintptr_t xl_up;
    uintptr_t locked_mask;
    gf_lock_t lock;
    xlator_t ** xl_list;
} ida_private_t;

union _ida_args
{
    ida_args_lookup_t      lookup;
    ida_args_stat_t        stat;
    ida_args_truncate_t    truncate;
    ida_args_access_t      access;
    ida_args_readlink_t    readlink;
    ida_args_mknod_t       mknod;
    ida_args_mkdir_t       mkdir;
    ida_args_unlink_t      unlink;
    ida_args_rmdir_t       rmdir;
    ida_args_symlink_t     symlink;
    ida_args_rename_t      rename;
    ida_args_link_t        link;
    ida_args_create_t      create;
    ida_args_open_t        open;
    ida_args_readv_t       readv;
    ida_args_writev_t      writev;
    ida_args_flush_t       flush;
    ida_args_fsync_t       fsync;
    ida_args_opendir_t     opendir;
    ida_args_readdir_t     readdir;
    ida_args_statfs_t      statfs;
    ida_args_setxattr_t    setxattr;
    ida_args_getxattr_t    getxattr;
    ida_args_removexattr_t removexattr;
    ida_args_lk_t          lk;
    ida_args_inodelk_t     inodelk;
    ida_args_entrylk_t     entrylk;
    ida_args_rchecksum_t   rchecksum;
    ida_args_xattrop_t     xattrop;
    ida_args_setattr_t     setattr;
};

struct _ida_args_cbk
{
    struct list_head list;
    int32_t          result;
    int32_t          code;
    uint32_t         index;
    uint32_t         mixed_count;
    uintptr_t        mixed_mask;
    uint64_t         heal_available;
    union
    {
        ida_args_lookup_cbk_t      lookup;
        ida_args_stat_cbk_t        stat;
        ida_args_truncate_cbk_t    truncate;
        ida_args_access_cbk_t      access;
        ida_args_readlink_cbk_t    readlink;
        ida_args_mknod_cbk_t       mknod;
        ida_args_mkdir_cbk_t       mkdir;
        ida_args_unlink_cbk_t      unlink;
        ida_args_rmdir_cbk_t       rmdir;
        ida_args_symlink_cbk_t     symlink;
        ida_args_rename_cbk_t      rename;
        ida_args_link_cbk_t        link;
        ida_args_create_cbk_t      create;
        ida_args_open_cbk_t        open;
        ida_args_readv_cbk_t       readv;
        ida_args_writev_cbk_t      writev;
        ida_args_flush_cbk_t       flush;
        ida_args_fsync_cbk_t       fsync;
        ida_args_opendir_cbk_t     opendir;
        ida_args_readdir_cbk_t     readdir;
        ida_args_statfs_cbk_t      statfs;
        ida_args_setxattr_cbk_t    setxattr;
        ida_args_getxattr_cbk_t    getxattr;
        ida_args_removexattr_cbk_t removexattr;
        ida_args_lk_cbk_t          lk;
        ida_args_inodelk_cbk_t     inodelk;
        ida_args_entrylk_cbk_t     entrylk;
        ida_args_rchecksum_cbk_t   rchecksum;
        ida_args_xattrop_cbk_t     xattrop;
        ida_args_setattr_cbk_t     setattr;
    };
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
    ida_args_cbk_t * cbk;
    ida_args_t args;
    ida_args_cbk_t args_cbk[0];
};

#define ida_mask_for_each(_index, _mask) \
    for ((_index) = (ffsll(_mask) - 1) & 127; (_index) < 64; (_index)++, (_index) += (ffsll((_mask) >> (_index)) - 1) & 127)

#define ida_args_cbk_for_each(_item, _local) \
    list_for_each(_item, &(_local)->args_groups)

#define ida_args_cbk_for_each_entry(_args, _local) \
    list_for_each_entry(_args, &(_local)->args_groups, list)

int32_t ida_nest(ida_local_t ** local, xlator_t * xl, call_frame_t * base, inode_t * inode, ida_manager_t * manager, int32_t required, uintptr_t mask, ida_callback_f callback);
void ida_ref(ida_local_t * local);
void ida_unref(ida_local_t * local);
int32_t ida_dispatch(ida_local_t * local, int32_t index, ida_args_t * args);
void ida_execute(ida_local_t * local);
void ida_report(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args);
ida_local_t * ida_process(ida_args_cbk_t ** args, xlator_t * xl, call_frame_t * frame, void * cookie, int32_t result, int32_t code, dict_t * xdata);
void ida_combine(ida_local_t * local, ida_args_cbk_t * args, int32_t error);
void ida_complete(ida_local_t * local, int32_t error);

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

#endif /* __IDA_MANAGER_H__ */
