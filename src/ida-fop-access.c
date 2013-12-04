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
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_access_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = ida_dict_assign(local, &args->access.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_access(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(access, frame, child, index, ida_dispatch_access_cbk, &args->access.loc, args->access.mode, args->access.xdata);

    return 0;
}

static int32_t ida_combine_access(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->access.xdata, src->access.xdata);
    }

    return error;
}

static int32_t ida_rebuild_access(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_access(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_access(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.access.loc);
    ida_dict_unassign(&local->args.access.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->access.xdata);
    }
}

static ida_manager_t ida_manager_access =
{
    .name     = "access",
    .dispatch = ida_dispatch_access,
    .combine  = ida_combine_access,
    .rebuild  = ida_rebuild_access,
    .finish   = ida_finish_access,
    .wipe     = ida_wipe_access,
};

void ida_execute_access(ida_local_t * local, loc_t * loc, int32_t mode, dict_t * xdata)
{
    int32_t error;

    local->args.access.mode = mode;
    error = ida_loc_assign(local, &local->args.access.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.access.xdata, xdata);
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

static void ida_callback_access(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(access, local->next, -1, error, NULL);
    }
    else
    {
        IDA_UNWIND(access, local->next, args->result, args->code, args->access.xdata);
    }
}

int32_t ida_nest_access(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, int32_t mode, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_access, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_access(new_local, loc, mode, xdata);
    }

    return error;
}

int32_t ida_access(call_frame_t * frame, xlator_t * this, loc_t * loc, int32_t mode, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_access, 0, 0, ida_callback_access);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(access, frame, -1, error, NULL);
    }
    else
    {
        ida_execute_access(local, loc, mode, xdata);
    }

    return 0;
}
