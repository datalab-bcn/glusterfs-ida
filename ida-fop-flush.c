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
#include "ida-type-iatt.h"
#include "ida-type-fd.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_flush_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, dict_t * xdata)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xdata);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = ida_dict_assign(local, &args->flush.xdata, xdata);

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_flush(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(flush, frame, child, index, ida_dispatch_flush_cbk, args->flush.fd, args->flush.xdata);

    return 0;
}

static int32_t ida_combine_flush(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
{
    int32_t error;

    error = 0;
    if ((dst->result != src->result) || (dst->code != src->code))
    {
        error = EINVAL;
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->flush.xdata, src->flush.xdata);
    }

    return error;
}

static int32_t ida_rebuild_flush(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    int32_t index;

//    ida_mask_for_each(index, mask)
//    {
//        gf_log(local->xl->name, GF_LOG_DEBUG, "index: %u", index);
//    }

    return 0;
}

static void ida_finish_flush(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_flush(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.flush.fd);
    ida_dict_unassign(&local->args.flush.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_dict_unassign(&args->access.xdata);
    }
}

static ida_manager_t ida_manager_flush =
{
    .name     = "flush",
    .dispatch = ida_dispatch_flush,
    .combine  = ida_combine_flush,
    .rebuild  = ida_rebuild_flush,
    .finish   = ida_finish_flush,
    .wipe     = ida_wipe_flush,
};

void ida_execute_flush(ida_local_t * local, fd_t * fd, dict_t * xdata)
{
    int32_t error;

    error = ida_fd_assign(local, &local->args.flush.fd, fd);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.flush.xdata, xdata);
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

static void ida_callback_flush(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(flush, local->next, -1, error, NULL);
    }
    else
    {
        IDA_UNWIND(flush, local->next, args->result, args->code, args->flush.xdata);
    }
}

int32_t ida_nest_flush(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_flush, required, mask, callback);
    if (likely(error == 0))
    {
        ida_execute_flush(new_local, fd, xdata);
    }

    return error;
}

int32_t ida_flush(call_frame_t * frame, xlator_t * this, fd_t * fd, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_flush, IDA_EXECUTE_MAX, 0, ida_callback_flush);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(flush, frame, -1, error, NULL);
    }
    else
    {
        ida_execute_flush(local, fd, xdata);
    }

    return 0;
}
