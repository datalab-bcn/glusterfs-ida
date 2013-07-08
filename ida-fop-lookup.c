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
#include "ida-check.h"
#include "ida-memory.h"
#include "ida-manager.h"
#include "ida-rabin.h"
#include "ida-heal.h"

static int32_t ida_dispatch_lookup_cbk(call_frame_t * frame, void * cookie, xlator_t * this, int32_t result, int32_t code, inode_t * inode, struct iatt * attr, dict_t * xattr, struct iatt * attr_ppost)
{
    ida_local_t * local;
    ida_args_cbk_t * args;
    int32_t error;

    local = ida_process(&args, this, frame, cookie, result, code, xattr);
    if (unlikely(local == NULL))
    {
        return 0;
    }

    error = 0;

    if (result >= 0)
    {
        error = ida_inode_assign(local, &args->lookup.inode, inode);
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->lookup.attr, attr);
        }
        if (likely(error == 0))
        {
            error = ida_dict_assign(local, &args->lookup.xattr, xattr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_assign(local, &args->lookup.attr_ppost, attr_ppost);
        }
    }

    ida_combine(local, args, error);

    return 0;
}

static int32_t ida_dispatch_lookup(ida_local_t * local, xlator_t * child, call_frame_t * frame, int32_t index, ida_args_t * args)
{
    IDA_WIND(lookup, frame, child, index, ida_dispatch_lookup_cbk, &args->lookup.loc, args->lookup.xattr);

    return 0;
}

static int32_t ida_combine_lookup(ida_local_t * local, ida_args_cbk_t * dst, ida_args_cbk_t * src)
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
            error = ida_inode_combine(local, dst->lookup.inode, src->lookup.inode);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->lookup.attr, &src->lookup.attr);
        }
        if (likely(error == 0))
        {
            error = ida_iatt_combine(local, &dst->lookup.attr_ppost, &src->lookup.attr_ppost);
        }
    }
    if (likely(error == 0))
    {
        error = ida_dict_combine_cow(local, &dst->lookup.xattr, src->lookup.xattr);
    }

    return error;
}

static int32_t ida_rebuild_lookup(ida_local_t * local, uintptr_t mask, ida_args_cbk_t * args)
{
    int32_t index, count;
    data_t * data;
    size_t size, bsize;
    uint8_t * block;
    uint8_t * blocks[16];
    uint32_t values[16];

    gf_log(local->xl->name, GF_LOG_DEBUG, "lookup size: %ld", args->lookup.attr.ia_size);
    ida_iatt_adjust(local, &args->lookup.attr, args->lookup.xattr, args->lookup.inode);
    gf_log(local->xl->name, GF_LOG_DEBUG, "lookup size: %ld", args->lookup.attr.ia_size);

    local->inode_ctx->size = args->lookup.attr.ia_size;

    if (args->lookup.xattr == NULL)
    {
        return 0;
    }

    data = dict_get(args->lookup.xattr, GF_CONTENT_KEY);
    if (data != NULL)
    {
        size = 0;
        count = 0;
        ida_mask_for_each(index, mask)
        {
            data = dict_get(local->args_cbk[index].lookup.xattr, GF_CONTENT_KEY);
            if (unlikely(data == NULL))
            {
                gf_log(local->xl->name, GF_LOG_ERROR, "Unexpected failure to retrieve the dictionary key '" GF_CONTENT_KEY "'");

                dict_del(args->lookup.xattr, GF_CONTENT_KEY);

                return 0;
            }
            bsize = ida_size_adjust(local, args->lookup.attr.ia_size);
            size = data->len;
            if (size > bsize)
            {
                size = bsize;
            }
            if ((size % (16 * IDA_RABIN_BITS)) != 0)
            {
                gf_log(local->xl->name, GF_LOG_ERROR, "Size of '" GF_CONTENT_KEY "' is not valid");

                dict_del(args->lookup.xattr, GF_CONTENT_KEY);

                return 0;
            }
            blocks[count] = (uint8_t *)data->data;
            values[count] = index;
            count++;
        }

        block = IDA_CALLOC_OR_RETURN_ERROR(local->xl->name, size * local->private->fragments, uint8_t, -ENOMEM);
        ida_rabin_merge(size, local->private->fragments, values, blocks, block);

        size = local->args.lookup.content;
        if (size > args->lookup.attr.ia_size)
        {
            size = args->lookup.attr.ia_size;
        }
        if (ida_dict_set_bin_cow(&args->lookup.xattr, GF_CONTENT_KEY, block, size) != 0)
        {
            GF_FREE(block);

            dict_del(args->lookup.xattr, GF_CONTENT_KEY);

            return 0;
        }
    }

    return 0;
}

static void ida_finish_lookup(ida_local_t * local)
{
    ida_args_cbk_t * args, * group;
    uintptr_t others_mask;
    int32_t others_count;

    args = local->cbk;

    if (args != NULL)
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "lookup_finish('%s', '%s') = %d, %d", local->args.lookup.loc.path, local->args.lookup.loc.name, args->result, args->code);

        if ((args->result >= 0) && (args->lookup.attr.ia_type == IA_IFREG))
        {
            others_count = 0;
            others_mask = 0;
            ida_args_cbk_for_each_entry(group, local)
            {
                if ((group != args) && ((group->result >= 0) || (group->code != ENOTCONN)))
                {
                    others_count += group->mixed_count;
                    others_mask |= group->mixed_mask;
                }
            }

            gf_log(local->xl->name, GF_LOG_DEBUG, "lookup masks: %lX(%d) %lX(%d)", args->mixed_mask, args->mixed_count, others_mask, others_count);

            if (unlikely(others_count != 0))
            {
                ida_heal_start(local, args, others_mask);
            }
        }
    }
    else
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "lookup_finish('%s', '%s') = no valid answer", local->args.lookup.loc.path, local->args.lookup.loc.name);
    }

    ida_unref(local);
}

static void ida_wipe_lookup(ida_local_t * local)
{
    ida_args_cbk_t * args;

    ida_loc_unassign(&local->args.lookup.loc);
    ida_dict_unassign(&local->args.lookup.xattr);

    ida_args_cbk_for_each_entry(args, local)
    {
        ida_inode_unassign(&args->lookup.inode);
        ida_iatt_unassign(&args->lookup.attr);
        ida_dict_unassign(&args->lookup.xattr);
        ida_iatt_unassign(&args->lookup.attr_ppost);
    }
}

static ida_manager_t ida_manager_lookup =
{
    .name     = "lookup",
    .dispatch = ida_dispatch_lookup,
    .combine  = ida_combine_lookup,
    .rebuild  = ida_rebuild_lookup,
    .finish   = ida_finish_lookup,
    .wipe     = ida_wipe_lookup,
};

static void ida_execute_lookup(ida_local_t * local, loc_t * loc, dict_t * xattr)
{
    int32_t error;

    error = ida_loc_assign(local, &local->args.lookup.loc, loc);
    if (likely(error == 0))
    {
        error = ida_dict_new(local, &local->args.lookup.xattr, xattr);
    }
    if (likely(error == 0))
    {
        error = ida_dict_set_uint64_cow(&local->args.lookup.xattr, IDA_KEY_VERSION, 0);
    }
    if (likely(error == 0))
    {
        error = ida_dict_set_uint64_cow(&local->args.lookup.xattr, IDA_KEY_SIZE, 0);
    }
    if (likely(error == 0))
    {
        error = ida_xattr_update_content(local, local->args.lookup.xattr, &local->args.lookup.content);
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

static void ida_callback_lookup(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    if (unlikely(error != 0))
    {
        IDA_UNWIND(lookup, local->next, -1, error, NULL, NULL, NULL, NULL);
    }
    else
    {
        IDA_UNWIND(lookup, local->next, args->result, args->code, args->lookup.inode, &args->lookup.attr, args->lookup.xattr, &args->lookup.attr_ppost);
    }
}

int32_t ida_nest_lookup(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, dict_t * xattr)
{
    ida_local_t * new_local;
    int32_t error;

//    gf_log(local->xl->name, GF_LOG_DEBUG, "nest_lookup('%s', '%s')", loc->path, loc->name);
    error = ida_nest(&new_local, local->xl, local->frame, loc->inode, &ida_manager_lookup, required, mask, callback);
    if (error == 0)
    {
        ida_execute_lookup(new_local, loc, xattr);
    }

    return error;
}

int32_t ida_lookup(call_frame_t * frame, xlator_t * this, loc_t * loc, dict_t * xdata)
{
    ida_local_t * local;
    int32_t error;

//    gf_log(this->name, GF_LOG_DEBUG, "lookup('%s', '%s')", loc->path, loc->name);
    error = ida_nest(&local, this, frame, loc->inode, &ida_manager_lookup, IDA_EXECUTE_MAX, 0, ida_callback_lookup);
    if (unlikely(error != 0))
    {
        IDA_UNWIND(lookup, frame, -1, error, NULL, NULL, NULL, NULL);
    }
    else
    {
        ida_execute_lookup(local, loc, xdata);
    }

    return 0;
}
