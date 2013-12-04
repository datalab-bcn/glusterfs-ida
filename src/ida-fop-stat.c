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
#include "ida-flow.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_stat_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->stat.attr, attr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->stat.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_stat(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.stat.fd == NULL)
    {
        IDA_WIND(stat, frame, child, index, ida_dispatch_stat_cbk, &args->stat.loc, args->stat.xdata);
    }
    else
    {
        IDA_WIND(fstat, frame, child, index, ida_dispatch_stat_cbk, args->stat.fd, args->stat.xdata);
    }

    return 0;
}

static int32_t ida_combine_stat(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->stat.attr, &src->stat.attr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->stat.xdata, src->stat.xdata);
    }

    return error;
}

static int32_t ida_rebuild_stat(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    if (local->args.stat.fd == NULL)
    {
        ida_iatt_adjust(local, &args->stat.attr, NULL, local->args.stat.loc.inode);
    }
    else
    {
        ida_iatt_adjust(local, &args->stat.attr, NULL, local->args.stat.fd->inode);
    }

    return 0;
}

static void ida_finish_stat(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_stat(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.stat.loc);
    ida_fd_unassign(&local->args.stat.fd);
    ida_dict_unassign(&local->args.stat.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->stat.attr);
        ida_dict_unassign(&args->stat.xdata);
    }
}

static ida_manager_t ida_manager_stat =
{
    .name     = "stat",
    .dispatch = ida_dispatch_stat,
    .combine  = ida_combine_stat,
    .rebuild  = ida_rebuild_stat,
    .finish   = ida_finish_stat,
    .wipe     = ida_wipe_stat,
};

static void ida_execute_stat(ida_local_t * local, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    int32_t error;

    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.stat.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.stat.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.stat.xdata, xdata);
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

static void ida_callback_stat(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.stat.fd == NULL)
        {
            IDA_UNWIND(stat, local->next, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fstat, local->next, -1, error, NULL, NULL);
        }
    }
    else
    {
        if (local->args.stat.fd == NULL)
        {
            IDA_UNWIND(stat, local->next, args->result, args->code, &args->stat.attr, args->stat.xdata);
        }
        else
        {
            IDA_UNWIND(fstat, local->next, args->result, args->code, &args->stat.attr, args->stat.xdata);
        }
    }
}

static int32_t ida_nest_stat_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_stat, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_stat(new_local, loc, fd, xdata);
    }

    return error;
}

int32_t ida_nest_stat(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, dict_t * xdata)
{
    return ida_nest_stat_common(local, required, mask, callback, loc->inode, loc, NULL, xdata);
}

int32_t ida_nest_fstat(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, dict_t * xdata)
{
    return ida_nest_stat_common(local, required, mask, callback, fd->inode, NULL, fd, xdata);
}

static int32_t ida_stat_common(call_frame_t * frame, xlator_t * this, inode_t * inode, loc_t * loc, fd_t * fd, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_stat, 0, 0, ida_callback_stat);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(stat, frame, -1, error, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fstat, frame, -1, error, NULL, NULL);
        }
    }
    else
    {
        ida_execute_stat(local, loc, fd, xdata);
    }

    return 0;
}

int32_t ida_stat(call_frame_t * frame, xlator_t * this, loc_t * loc, dict_t * xdata)
{
    return ida_stat_common(frame, this, loc->inode, loc, NULL, xdata);
}

int32_t ida_fstat(call_frame_t * frame, xlator_t * this, fd_t * fd, dict_t * xdata)
{
    return ida_stat_common(frame, this, fd->inode, NULL, fd, xdata);
}

static int32_t ida_dispatch_setattr_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, struct iatt * attr_pre, struct iatt * attr_post, dict_t * xdata)
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
        error = ida_iatt_assign(local, &args->setattr.attr_pre, attr_pre);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->setattr.attr_post, attr_post);
        }
    }
    if (likely(error  == 0))
    {
        error = ida_dict_assign(local, &args->setattr.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_setattr(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    if (local->args.setattr.fd == NULL)
    {
        IDA_WIND(setattr, frame, child, index, ida_dispatch_setattr_cbk, &args->setattr.loc, &args->setattr.attr, args->setattr.valid, args->setattr.xdata);
    }
    else
    {
        IDA_WIND(fsetattr, frame, child, index, ida_dispatch_setattr_cbk, args->setattr.fd, &args->setattr.attr, args->setattr.valid, args->setattr.xdata);
    }

    return 0;
}

static int32_t ida_combine_setattr(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_iatt_combine(local, &dst->setattr.attr_pre, &src->setattr.attr_pre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->setattr.attr_post, &src->setattr.attr_post);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->setattr.xdata, src->setattr.xdata);
    }

    return error;
}

static int32_t ida_rebuild_setattr(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_setattr(ida_local_t * local)
{
    ida_flow_inode_unlock(local, NULL, &local->args.setattr.loc, local->args.setattr.fd, 0, 0);
}

static void ida_wipe_setattr(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.setattr.loc);
    ida_fd_unassign(&local->args.setattr.fd);
    ida_iatt_unassign(&local->args.setattr.attr);
    ida_dict_unassign(&local->args.setattr.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_iatt_unassign(&args->setattr.attr_pre);
        ida_iatt_unassign(&args->setattr.attr_post);
        ida_dict_unassign(&args->setattr.xdata);
    }
}

static ida_manager_t ida_manager_setattr =
{
    .name     = "setattr",
    .dispatch = ida_dispatch_setattr,
    .combine  = ida_combine_setattr,
    .rebuild  = ida_rebuild_setattr,
    .finish   = ida_finish_setattr,
    .wipe     = ida_wipe_setattr,
};

static void ida_execute_setattr(ida_local_t * local, loc_t * loc, fd_t * fd, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    int32_t error;

    local->args.setattr.valid = valid;
    if (fd == NULL)
    {
        error = ida_loc_assign(local, &local->args.setattr.loc, loc);
    }
    else
    {
        error = ida_fd_assign(local, &local->args.setattr.fd, fd);
    }
    if (likely(error == 0))
    {
        error = ida_iatt_assign(local, &local->args.setattr.attr, attr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.setattr.xdata, xdata);
    }
    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        ida_flow_inode_lock(local, NULL, &local->args.setattr.loc, local->args.setattr.fd, 0, 0);
    }
}

static void ida_callback_setattr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        if (local->args.setattr.fd == NULL)
        {
            IDA_UNWIND(setattr, local->next, -1, error, NULL, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fsetattr, local->next, -1, error, NULL, NULL, NULL);
        }
    }
    else
    {
        if (local->args.setattr.fd == NULL)
        {
            IDA_UNWIND(setattr, local->next, args->result, args->code, &args->setattr.attr_pre, &args->setattr.attr_post, args->setattr.xdata);
        }
        else
        {
            IDA_UNWIND(fsetattr, local->next, args->result, args->code, &args->setattr.attr_pre, &args->setattr.attr_post, args->setattr.xdata);
        }
    }
}

static int32_t ida_nest_setattr_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, inode, &ida_manager_setattr, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_setattr(new_local, loc, fd, attr, valid, xdata);
    }

    return error;
}

int32_t ida_nest_setattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    return ida_nest_setattr_common(local, required, mask, callback, loc->inode, loc, NULL, attr, valid, xdata);
}

int32_t ida_nest_fsetattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    return ida_nest_setattr_common(local, required, mask, callback, fd->inode, NULL, fd, attr, valid, xdata);
}

static int32_t ida_setattr_common(call_frame_t * frame, xlator_t * this, inode_t * inode, loc_t * loc, fd_t * fd, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, inode, &ida_manager_setattr, 0, 0, ida_callback_setattr);
    if (unlikely(error != 0))
    {
        if (fd == NULL)
        {
            IDA_UNWIND(setattr, frame, -1, error, NULL, NULL, NULL);
        }
        else
        {
            IDA_UNWIND(fsetattr, frame, -1, error, NULL, NULL, NULL);
        }
    }
    else
    {
        ida_execute_setattr(local, loc, fd, attr, valid, xdata);
    }

    return 0;
}

int32_t ida_setattr(call_frame_t * frame, xlator_t * this, loc_t * loc, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    return ida_setattr_common(frame, this, loc->inode, loc, NULL, attr, valid, xdata);
}

int32_t ida_fsetattr(call_frame_t * frame, xlator_t * this, fd_t * fd, struct iatt * attr, int32_t valid, dict_t * xdata)
{
    return ida_setattr_common(frame, this, fd->inode, NULL, fd, attr, valid, xdata);
}
