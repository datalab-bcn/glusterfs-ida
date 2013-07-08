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

#include "ida.h"
#include "ida-common.h"
#include "ida-type-loc.h"
#include "ida-type-dict.h"
#include "ida-type-inode.h"
#include "ida-type-fd.h"
#include "ida-type-iatt.h"
#include "ida-type-str.h"
#include "ida-flow.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_getxattr_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xattr, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = 0;

    if (result >= 0)
    {
        error = ida_dict_assign(local, &args->getxattr.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->getxattr.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_getxattr(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.getxattr.fd == NULL)
    {
        IDA_WIND(getxattr, frame, child, index, ida_dispatch_getxattr_cbk, &args->getxattr.loc, args->getxattr.name, args->getxattr.xdata);
    }
    else
    {
        IDA_WIND(fgetxattr, frame, child, index, ida_dispatch_getxattr_cbk, args->getxattr.fd, args->getxattr.name, args->getxattr.xdata);
    }

    return 0;
}

static int32_t ida_combine_getxattr(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (dst->result >= 0)
    {
        if (likely(error == 0))
        {
            error = ida_dict_combine_cow(local, &dst->getxattr.xattr, src->getxattr.xattr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->getxattr.xdata, src->getxattr.xdata);
    }

    return error;
}

static int32_t ida_rebuild_getxattr(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_getxattr(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_getxattr(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.getxattr.loc);
    ida_fd_unassign(&local->args.getxattr.fd);
    ida_str_unassign(&local->args.getxattr.name);
    ida_dict_unassign(&local->args.getxattr.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->getxattr.xattr);
        ida_dict_unassign(&args->getxattr.xdata);
    }
}

static ida_manager_t ida_manager_getxattr =
{
    .name     = "getxattr",
    .dispatch = ida_dispatch_getxattr,
    .combine  = ida_combine_getxattr,
    .rebuild  = ida_rebuild_getxattr,
    .finish   = ida_finish_getxattr,
    .wipe     = ida_wipe_getxattr,
};

void ida_execute_getxattr(ida_local_t * local, loc_t * loc, fd_t * fd, const char * name, dict_t * xdata)
{
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.getxattr.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.getxattr.fd, fd);
    }
    if (likely(error == 0))
    {
        if (name != NULL)
        {
            error = ida_str_assign(local, &local->args.getxattr.name, name);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.getxattr.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_getxattr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.getxattr.fd == NULL)
        {
            IDA_UNWIND(getxattr, local->next, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fgetxattr, local->next, -1, error, NULL, NULL);
        }
    }
    else
    {
        if (local->args.getxattr.fd == NULL)
        {
            IDA_UNWIND(getxattr, local->next, args->result, args->code, args->getxattr.xattr, args->getxattr.xdata);
        }
        else
        {
            IDA_UNWIND(fgetxattr, local->next, args->result, args->code, args->getxattr.xattr, args->getxattr.xdata);
        }
    }
}

static int32_t ida_nest_getxattr_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, const char * name, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_getxattr, required, mask, callback);
    if (error == 0)
    {
        ida_execute_getxattr(new_local, loc, fd, name, xdata);
    }

    return error;
}

int32_t ida_nest_getxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, const char * name, dict_t * xdata)
{
    return ida_nest_getxattr_common(local, required, mask, callback, loc->inode, loc, NULL, name, xdata);
}

int32_t ida_nest_fgetxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, const char * name, dict_t * xdata)
{
    return ida_nest_getxattr_common(local, required, mask, callback, fd->inode, NULL, fd, name, xdata);
}

static int32_t ida_getxattr_common(call_frame_t * frame, xlator_t * this, inode_t * inode, loc_t * loc, fd_t * fd, const char * name, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_getxattr, 0, 0, ida_callback_getxattr);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(getxattr, frame, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fgetxattr, frame, -1, error, NULL, NULL);
        }
    }
    else
    {
        ida_execute_getxattr(local, loc, fd, name, xdata);
    }

    return 0;
}

int32_t ida_getxattr(call_frame_t * frame, xlator_t * this, loc_t * loc, const char * name, dict_t * xdata)
{
    return ida_getxattr_common(frame, this, loc->inode, loc, NULL, name, xdata);
}

int32_t ida_fgetxattr(call_frame_t * frame, xlator_t * this, fd_t * fd, const char * name, dict_t * xdata)
{
    return ida_getxattr_common(frame, this, fd->inode, NULL, fd, name, xdata);
}

static int32_t ida_dispatch_setxattr_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = ida_dict_assign(local, &args->setxattr.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_setxattr(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.getxattr.fd == NULL)
    {
        IDA_WIND(setxattr, frame, child, index, ida_dispatch_setxattr_cbk, &args->setxattr.loc, args->setxattr.xattr, args->setxattr.flags, args->setxattr.xdata);
    }
    else
    {
        IDA_WIND(fsetxattr, frame, child, index, ida_dispatch_setxattr_cbk, args->setxattr.fd, args->setxattr.xattr, args->setxattr.flags, args->setxattr.xdata);
    }

    return 0;
}

static int32_t ida_combine_setxattr(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->setxattr.xdata, src->setxattr.xdata);
    }

    return error;
}

static int32_t ida_rebuild_setxattr(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_setxattr(ida_local_t * local)
{
    if (!local->healing)
    {
        ida_flow_inode_unlock(local, NULL, &local->args.setxattr.loc, local->args.setxattr.fd, 0, 0);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_setxattr(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.setxattr.loc);
    ida_fd_unassign(&local->args.setxattr.fd);
    ida_dict_unassign(&local->args.setxattr.xattr);
    ida_dict_unassign(&local->args.setxattr.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->setxattr.xdata);
    }
}

static ida_manager_t ida_manager_setxattr =
{
    .name     = "setxattr",
    .dispatch = ida_dispatch_setxattr,
    .combine  = ida_combine_setxattr,
    .rebuild  = ida_rebuild_setxattr,
    .finish   = ida_finish_setxattr,
    .wipe     = ida_wipe_setxattr,
};

void ida_execute_setxattr(ida_local_t * local, loc_t * loc, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    int32_t error;

    local->args.setxattr.flags = flags;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.setxattr.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.setxattr.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.setxattr.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.setxattr.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_flow_inode_lock(local, NULL, &local->args.setxattr.loc, local->args.setxattr.fd, 0, 0);
    }
}

void ida_execute_setxattr_heal(ida_local_t * local, loc_t * loc, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    int32_t error;

    local->args.setxattr.flags = flags;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.setxattr.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.setxattr.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.setxattr.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.setxattr.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_execute(local);
    }
}

static void ida_callback_setxattr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.setxattr.fd == NULL)
        {
            IDA_UNWIND(setxattr, local->next, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(fsetxattr, local->next, -1, error, NULL);
        }
    }
    else
    {
        if (local->args.setxattr.fd == NULL)
        {
            IDA_UNWIND(setxattr, local->next, args->result, args->code, args->setxattr.xdata);
        }
        else
        {
            IDA_UNWIND(fsetxattr, local->next, args->result, args->code, args->setxattr.xdata);
        }
    }
}

int32_t ida_nest_setxattr_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_setxattr, required, mask, callback);
    if (error == 0)
    {
        if (!local->healing)
        {
            ida_execute_setxattr(new_local, loc, fd, xattr, flags, xdata);
        }
        else
        {
            ida_execute_setxattr_heal(new_local, loc, fd, xattr, flags, xdata);
        }
    }

    return error;
}

int32_t ida_nest_setxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    return ida_nest_setxattr_common(local, required, mask, callback, loc->inode, loc, NULL, xattr, flags, xdata);
}

int32_t ida_nest_fsetxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    return ida_nest_setxattr_common(local, required, mask, callback, fd->inode, NULL, fd, xattr, flags, xdata);
}

static int32_t ida_setxattr_common(call_frame_t * frame, xlator_t * this, loc_t * loc, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_setxattr, IDA_EXECUTE_MAX, 0, ida_callback_setxattr);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(setxattr, frame, -1, error, NULL);
        }
        else
        {
            IDA_UNWIND(fsetxattr, frame, -1, error, NULL);
        }
    }
    else
    {
        ida_execute_setxattr(local, loc, fd, xattr, flags, xdata);
    }

    return 0;
}

int32_t ida_setxattr(call_frame_t * frame, xlator_t * this, loc_t * loc, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    return ida_setxattr_common(frame, this, loc, NULL, xattr, flags, xdata);
}

int32_t ida_fsetxattr(call_frame_t * frame, xlator_t * this, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata)
{
    return ida_setxattr_common(frame, this, NULL, fd, xattr, flags, xdata);
}

static int32_t ida_dispatch_removexattr_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = ida_dict_assign(local, &args->removexattr.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_removexattr(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(removexattr, frame, child, index, ida_dispatch_removexattr_cbk, &args->removexattr.loc, args->removexattr.name, args->removexattr.xdata);

    return 0;
}

static int32_t ida_combine_removexattr(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->removexattr.xdata, src->removexattr.xdata);
    }

    return error;
}

static int32_t ida_rebuild_removexattr(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_removexattr(ida_local_t * local)
{
    ida_flow_inode_unlock(local, NULL, &local->args.removexattr.loc, NULL, 0, 0);
}

static void ida_wipe_removexattr(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.removexattr.loc);
    ida_str_unassign(&local->args.removexattr.name);
    ida_dict_unassign(&local->args.removexattr.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->removexattr.xdata);
    }
}

static ida_manager_t ida_manager_removexattr =
{
    .name     = "removexattr",
    .dispatch = ida_dispatch_removexattr,
    .combine  = ida_combine_removexattr,
    .rebuild  = ida_rebuild_removexattr,
    .finish   = ida_finish_removexattr,
    .wipe     = ida_wipe_removexattr,
};

void ida_execute_removexattr(ida_local_t * local, loc_t * loc, const char * name, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.removexattr.loc, loc);
    if (likely(error == 0))
    {
        error = ida_str_assign(local, &local->args.removexattr.name, name);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.removexattr.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_flow_inode_lock(local, NULL, &local->args.removexattr.loc, NULL, 0, 0);
    }
}

static void ida_callback_removexattr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(removexattr, local->next, -1, error, NULL);
    }
    else
    {
        IDA_UNWIND(removexattr, local->next, args->result, args->code, args->removexattr.xdata);
    }
}

int32_t ida_nest_removexattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, const char * name, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_removexattr, required, mask, callback);
    if (error == 0)
    {
        ida_execute_removexattr(new_local, loc, name, xdata);
    }

    return error;
}

int32_t ida_removexattr(call_frame_t * frame, xlator_t * this, loc_t * loc, const char * name, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_removexattr, IDA_EXECUTE_MAX, 0, ida_callback_removexattr);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(removexattr, frame, -1, error, NULL);
    }
    else
    {
        ida_execute_removexattr(local, loc, name, xdata);
    }

    return 0;
}

static int32_t ida_dispatch_xattrop_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xattr, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = 0;

    if (result >= 0)
    {
        error = ida_dict_assign(local, &args->xattrop.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->xattrop.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_xattrop(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.xattrop.fd == NULL)
    {
        IDA_WIND(xattrop, frame, child, index, ida_dispatch_xattrop_cbk, &args->xattrop.loc, args->xattrop.flags, args->xattrop.xattr, args->xattrop.xdata);
    }
    else
    {
        IDA_WIND(fxattrop, frame, child, index, ida_dispatch_xattrop_cbk, args->xattrop.fd, args->xattrop.flags, args->xattrop.xattr, args->xattrop.xdata);
    }

    return 0;
}

static int32_t ida_combine_xattrop(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (dst->result >= 0)
    {
        if (likely(error == 0))
        {
            error = ida_dict_combine_cow(local, &dst->getxattr.xattr, src->getxattr.xattr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->getxattr.xdata, src->getxattr.xdata);
    }

    return error;
}

static int32_t ida_rebuild_xattrop(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_xattrop(ida_local_t * local)
{
    if (local->args.xattrop.lock)
    {
        ida_flow_inode_unlock(local, NULL, &local->args.xattrop.loc, local->args.xattrop.fd, 0, 0);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_xattrop(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.xattrop.loc);
    ida_fd_unassign(&local->args.xattrop.fd);
    ida_dict_unassign(&local->args.xattrop.xattr);
    ida_dict_unassign(&local->args.xattrop.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->xattrop.xattr);
        ida_dict_unassign(&args->xattrop.xdata);
    }
}

static ida_manager_t ida_manager_xattrop =
{
    .name     = "xattrop",
    .dispatch = ida_dispatch_xattrop,
    .combine  = ida_combine_xattrop,
    .rebuild  = ida_rebuild_xattrop,
    .finish   = ida_finish_xattrop,
    .wipe     = ida_wipe_xattrop,
};

void ida_execute_xattrop(ida_local_t * local, loc_t * loc, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata)
{
    int32_t error;

    local->args.xattrop.lock = lock;
    local->args.xattrop.flags = flags;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.xattrop.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.xattrop.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.xattrop.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.xattrop.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        if (lock)
        {
            ida_flow_inode_lock(local, NULL, &local->args.xattrop.loc, local->args.xattrop.fd, 0, 0);
        }
        else
        {
            ida_execute(local);
        }
    }
}

static void ida_callback_xattrop(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.xattrop.fd == NULL)
        {
            IDA_UNWIND(xattrop, local->next, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fxattrop, local->next, -1, error, NULL, NULL);
        }
    }
    else
    {
        if (local->args.xattrop.fd == NULL)
        {
            IDA_UNWIND(xattrop, local->next, args->result, args->code, args->xattrop.xattr, args->xattrop.xdata);
        }
        else
        {
            IDA_UNWIND(fxattrop, local->next, args->result, args->code, args->xattrop.xattr, args->xattrop.xdata);
        }
    }
}

int32_t ida_nest_xattrop_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_xattrop, required, mask, callback);
    if (error == 0)
    {
        ida_execute_xattrop(new_local, loc, fd, flags, xattr, lock, xdata);
    }

    return error;
}

int32_t ida_nest_xattrop(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata)
{
    return ida_nest_xattrop_common(local, required, mask, callback, loc->inode, loc, NULL, flags, xattr, lock, xdata);
}

int32_t ida_nest_fxattrop(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata)
{
    return ida_nest_xattrop_common(local, required, mask, callback, fd->inode, NULL, fd, flags, xattr, lock, xdata);
}

static int32_t ida_xattrop_common(call_frame_t * frame, xlator_t * this, inode_t * inode, loc_t * loc, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_xattrop, IDA_EXECUTE_MAX, 0, ida_callback_xattrop);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(xattrop, frame, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fxattrop, frame, -1, error, NULL, NULL);
        }
    }
    else
    {
        ida_execute_xattrop(local, loc, fd, flags, xattr, 1, xdata);
    }

    return 0;
}

int32_t ida_xattrop(call_frame_t * frame, xlator_t * this, loc_t * loc, gf_xattrop_flags_t flags, dict_t * xattr, dict_t * xdata)
{
    return ida_xattrop_common(frame, this, loc->inode, loc, NULL, flags, xattr, xdata);
}

int32_t ida_fxattrop(call_frame_t * frame, xlator_t * this, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, dict_t * xdata)
{
    return ida_xattrop_common(frame, this, fd->inode, NULL, fd, flags, xattr, xdata);
}
