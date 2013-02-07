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
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_readlink_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, const char * path, struct iatt * attr, dict_t * xdata)
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
        error = ida_str_assign(local, &args->readlink.path, path);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->readlink.attr, attr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->readlink.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_readlink(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(readlink, frame, child, index, ida_dispatch_readlink_cbk, &args->readlink.loc, args->readlink.size, args->readlink.xdata);

    return 0;
}

static int32_t ida_combine_readlink(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_str_combine(local, dst->readlink.path, src->readlink.path);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->readlink.attr, &src->readlink.attr);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->readlink.xdata, src->readlink.xdata);
    }

    return error;
}

static int32_t ida_rebuild_readlink(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_readlink(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_readlink(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.readlink.loc);
    ida_dict_unassign(&local->args.readlink.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_str_unassign(&args->readlink.path);
        ida_iatt_unassign(&args->readlink.attr);
        ida_dict_unassign(&args->readlink.xdata);
    }
}

static ida_manager_t ida_manager_readlink =
{
    .name     = "readlink",
    .dispatch = ida_dispatch_readlink,
    .combine  = ida_combine_readlink,
    .rebuild  = ida_rebuild_readlink,
    .finish   = ida_finish_readlink,
    .wipe     = ida_wipe_readlink,
};

void ida_execute_readlink(ida_local_t * local, loc_t * loc, size_t size, dict_t * xdata)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.readlink.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.readlink.xdata, xdata);
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

static void ida_callback_readlink(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readlink, local->next, -1, error, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(readlink, local->next, args->result, args->code, args->readlink.path, &args->readlink.attr, args->readlink.xdata);
    }
}

int32_t ida_nest_readlink(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, size_t size, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_readlink, required, mask, callback);
    if (error == 0)
    {
        ida_execute_readlink(new_local, loc, size, xdata);
    }

    return error;
}

int32_t ida_readlink(call_frame_t * frame, xlator_t * this, loc_t * loc, size_t size, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_readlink, 0, 0, ida_callback_readlink);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(readlink, frame, -1, error, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_readlink(local, loc, size, xdata);
    }

    return 0;
}
