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

static int32_t ida_dispatch_symlink_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, inode_t * inode, struct iatt * attr, struct iatt * attr_ppre, struct iatt * attr_ppost, dict_t * xdata)
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
        error = ida_inode_assign(local, &args->symlink.inode, inode);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->symlink.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->symlink.attr_ppre, attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->symlink.attr_ppost, attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->symlink.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_symlink(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(symlink, frame, child, index, ida_dispatch_symlink_cbk, args->symlink.name, &args->symlink.loc, args->symlink.umask, args->symlink.xdata);

    return 0;
}

static int32_t ida_combine_symlink(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_inode_combine(local, dst->symlink.inode, src->symlink.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->symlink.attr, &src->symlink.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->symlink.attr_ppre, &src->symlink.attr_ppre);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->symlink.attr_ppost, &src->symlink.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->symlink.xdata, src->symlink.xdata);
    }

    return error;
}

static int32_t ida_rebuild_symlink(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_symlink(ida_local_t * local)
{
    if (local->args.symlink.unlock)
    {
        ida_flow_entry_unlock(local, NULL, &local->args.symlink.loc, NULL, local->args.symlink.loc.name);
    }
    else
    {
        ida_unref(local);
    }
}

static void ida_wipe_symlink(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_str_unassign(&local->args.symlink.name);
    ida_loc_unassign(&local->args.symlink.loc);
    ida_dict_unassign(&local->args.symlink.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_inode_unassign(&args->symlink.inode);
        ida_iatt_unassign(&args->symlink.attr);
        ida_iatt_unassign(&args->symlink.attr_ppre);
        ida_iatt_unassign(&args->symlink.attr_ppost);
        ida_dict_unassign(&args->symlink.xdata);
    }
}

static ida_manager_t ida_manager_symlink =
{
    .name     = "symlink",
    .dispatch = ida_dispatch_symlink,
    .combine  = ida_combine_symlink,
    .rebuild  = ida_rebuild_symlink,
    .finish   = ida_finish_symlink,
    .wipe     = ida_wipe_symlink,
};

void ida_execute_symlink(ida_local_t * local, const char * name, loc_t * loc, mode_t umask, dict_t * params)
{
    int32_t error;

    local->args.symlink.umask = umask;
    error = ida_str_assign(local, &local->args.symlink.name, name);
    if (likely(error == 0))
    {
        error = ida_loc_assign(local, &local->args.symlink.loc, loc);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.symlink.xdata, params);
    }

    if (unlikely(error != 0))
    {
        ida_complete(local, error);
    }
    else
    {
        local->args.symlink.unlock = 1;
        ida_flow_entry_lock(local, NULL, &local->args.symlink.loc, NULL, local->args.symlink.loc.name);
    }
}

static void ida_callback_symlink(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(symlink, local->next, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(symlink, local->next, args->result, args->code, args->symlink.inode, &args->symlink.attr, &args->symlink.attr_ppre, &args->symlink.attr_ppost, args->symlink.xdata);
    }
}

int32_t ida_nest_symlink(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * name, loc_t * loc, mode_t umask, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_symlink, required, mask, callback);
    if (error == 0)
    {
        ida_execute_symlink(new_local, name, loc, umask, xdata);
    }

    return error;
}

int32_t ida_symlink(call_frame_t * frame, xlator_t * this, const char * name, loc_t * loc, mode_t umask, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_symlink, IDA_EXECUTE_MAX, 0, ida_callback_symlink);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(symlink, frame, -1, error, NULL, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_symlink(local, name, loc, umask, xdata);
    }

    return 0;
}
