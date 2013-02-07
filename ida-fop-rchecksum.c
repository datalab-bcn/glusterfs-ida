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
#include "ida-type-md5.h"
#include "ida-check.h"
#include "ida-manager.h"

static int32_t ida_dispatch_rchecksum_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, uint32_t weak, uint8_t * strong, dict_t * xdata)
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
        args->rchecksum.weak = weak;
        error = ida_md5_assign(local, &args->rchecksum.strong, strong);
    }
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &args->rchecksum.xdata, xdata);
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_rchecksum(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(rchecksum, frame, child, index, ida_dispatch_rchecksum_cbk, args->rchecksum.fd, args->rchecksum.offset, args->rchecksum.length, args->rchecksum.xdata);

    return 0;
}

static int32_t ida_combine_rchecksum(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_md5_combine(local, dst->rchecksum.strong, src->rchecksum.strong);
        }
    }

    return error;
}

static int32_t ida_rebuild_rchecksum(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    return 0;
}

static void ida_finish_rchecksum(ida_local_t * local)
{
    ida_unref(local);
}

static void ida_wipe_rchecksum(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_fd_unassign(&local->args.rchecksum.fd);
    ida_dict_unassign(&local->args.rchecksum.xdata);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_md5_unassign(&args->rchecksum.strong);
        ida_dict_unassign(&args->rchecksum.xdata);
    }
}

static ida_manager_t ida_manager_rchecksum =
{
    .name     = "rchecksum",
    .dispatch = ida_dispatch_rchecksum,
    .combine  = ida_combine_rchecksum,
    .rebuild  = ida_rebuild_rchecksum,
    .finish   = ida_finish_rchecksum,
    .wipe     = ida_wipe_rchecksum,
};

void ida_execute_rchecksum(ida_local_t * local, fd_t * fd, off_t offset, int32_t length, dict_t * xdata)
{
    int32_t error;

    local->args.rchecksum.offset = offset;
    local->args.rchecksum.length = length;
    error = ida_fd_assign(local, &local->args.rchecksum.fd, fd);
    if (likely(error == 0))
    {
        error = ida_dict_assign(local, &local->args.rchecksum.xdata, xdata);
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

static void ida_callback_rchecksum(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rchecksum, local->next, -1, error, 0, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(rchecksum, local->next, args->result, args->code, args->rchecksum.weak, args->rchecksum.strong, args->rchecksum.xdata);
    }
}

int32_t ida_nest_rchecksum(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, off_t offset, int32_t length, dict_t * xdata)
{
    ida_local_t * new_local;
    int32_t error;

    error = ida_nest(&new_local, local->xl, local->frame, fd->inode, &ida_manager_rchecksum, required, mask, callback);
    if (error == 0)
    {
        ida_execute_rchecksum(new_local, fd, offset, length, xdata);
    }

    return error;
}

int32_t ida_rchecksum(call_frame_t * frame, xlator_t * this, fd_t * fd, off_t offset, int32_t length, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

    error = ida_nest(&local, this, frame, fd->inode, &ida_manager_rchecksum, 0, 0, ida_callback_rchecksum);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(rchecksum, frame, -1, error, 0, NULL, NULL);
    }
    else
    {
        ida_execute_rchecksum(local, fd, offset, length, xdata);
    }

    return 0;
}
