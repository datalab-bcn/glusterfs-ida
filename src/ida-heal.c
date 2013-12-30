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

#include "gfsys.h"

#include "ida-common.h"
#include "ida-type-dict.h"
#include "ida-type-loc.h"
#include "ida-gf.h"
#include "ida-heal.h"
#include "ida.h"

void ida_heal_free(ida_inode_ctx_t * ctx)
{
    if (ctx->heal.src != NULL)
    {
        fd_unref(ctx->heal.src);
        ctx->heal.src = NULL;
    }
    if (ctx->heal.dst != NULL)
    {
        fd_unref(ctx->heal.dst);
        ctx->heal.dst = NULL;
    }
    if (ctx->heal.inode != NULL)
    {
        inode_unref(ctx->heal.inode);
    }
    ida_loc_unassign(&ctx->heal.loc);

    ctx->heal.healing = 0;
}

void ida_heal_unref(ida_inode_ctx_t * ctx)
{
    if (__sync_fetch_and_sub(&ctx->heal.refs, 1) == 1)
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_copy_write(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args);

void ida_heal_copy_read(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "copy read local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result > 0))
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "%u bytes written for healing", args->result);
        ida_nest_readv(local, sys_bits_count64(ctx->heal.src_mask), ctx->heal.src_mask, ida_heal_copy_write, ctx->heal.src, ctx->heal.size, ctx->heal.offset, 0, NULL);
    }
    else
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_done(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "done local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        gf_log(local->xl->name, GF_LOG_INFO, "Healing complete");
    }

    ida_heal_free(ctx);
}

void ida_heal_set_attr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "setattr local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        ida_nest_fsetattr(local, sys_bits_count64(ctx->heal.dst_mask), ctx->heal.dst_mask, ida_heal_done, ctx->heal.dst, &args->stat.attr, GF_SET_ATTR_MODE | GF_SET_ATTR_UID | GF_SET_ATTR_GID /*| GF_SET_ATTR_ATIME | GF_SET_ATTR_MTIME*/, NULL);
    }
    else
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_get_attr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "getattr local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        ida_nest_fstat(local, sys_bits_count64(ctx->heal.src_mask), ctx->heal.src_mask, ida_heal_set_attr, ctx->heal.src, NULL);
    }
    else
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_set_xattr(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "setxattr local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        ida_nest_fsetxattr(local, sys_bits_count64(ctx->heal.dst_mask), ctx->heal.dst_mask, ida_heal_get_attr, ctx->heal.dst, args->getxattr.xattr, 0, NULL);
    }
    else
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_copy_write(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;
    off_t offset;

    gf_log(local->xl->name, GF_LOG_DEBUG, "copy write local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result > 0))
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "%u bytes read for healing", args->result);
        offset = ctx->heal.offset;
        ctx->heal.offset += args->result;
        ida_nest_writev_heal(local, sys_bits_count64(ctx->heal.dst_mask), ctx->heal.dst_mask, ida_heal_copy_read, ctx->heal.dst, args->readv.buffer.vectors, args->readv.buffer.count, offset, 0, args->readv.buffer.buffers, NULL);
    }
    else if ((error == 0) && (args->result == 0))
    {
        ida_nest_fgetxattr(local, 1, ctx->heal.src_mask, ida_heal_set_xattr, ctx->heal.src, NULL, NULL);
    }
    else
    {
        ida_heal_free(ctx);
    }
}

void ida_heal_copy(ida_local_t * local)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "copy local: %s", local->manager.name);

    ctx = local->inode_ctx;
    ctx->heal.offset = 0;
    ida_nest_readv(local, sys_bits_count64(ctx->heal.src_mask), ctx->heal.src_mask, ida_heal_copy_write, ctx->heal.src, ctx->heal.size, ctx->heal.offset, 0, NULL);
}

void ida_heal_write(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "write local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        gf_log(local->xl->name, GF_LOG_INFO, "Heal create succeeded");
    }
    else
    {
        __sync_val_compare_and_swap(&ctx->heal.error, 0, error ? error : args->code);
        gf_log(local->xl->name, GF_LOG_ERROR, "Heal create failed (%d - %d, %d)", error, args->result, args->code);
    }
    if (__sync_fetch_and_sub(&ctx->heal.refs, 1) == 1)
    {
        if (ctx->heal.error != 0)
        {
            ida_heal_free(ctx);
        }
        else
        {
            ida_heal_copy(local);
        }
    }
}

void ida_heal_read(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;

    gf_log(local->xl->name, GF_LOG_DEBUG, "read local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && (args->result >= 0))
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "Heal open succeeded");
    }
    else
    {
        __sync_val_compare_and_swap(&ctx->heal.error, 0, error ? error : args->code);
        gf_log(local->xl->name, GF_LOG_ERROR, "Heal open failed");
    }
    if (__sync_fetch_and_sub(&ctx->heal.refs, 1) == 1)
    {
        if (ctx->heal.error != 0)
        {
            ida_heal_free(ctx);
        }
        else
        {
            ida_heal_copy(local);
        }
    }
}

void ida_heal_create(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;
    dict_t * xdata;
    uint8_t * data;

    gf_log(local->xl->name, GF_LOG_DEBUG, "create local: local=%p, private=%p, nodes=%u", local, local->private, local->private->nodes);

    ctx = local->inode_ctx;
    if (error == 0)
    {
        xdata = dict_new();
        if (unlikely(xdata == NULL))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to create a dict_t");

            ida_heal_unref(ctx);

            return;
        }
        data = GF_MALLOC(16, gf_ida_mt_uint8_t);
        if (unlikely(data == NULL))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to create a gfid");

            dict_unref(xdata);
            ida_heal_unref(ctx);

            return;
        }
        memcpy(data, ctx->heal.gfid, 16);
        error = ida_dict_set_bin_cow(&xdata, "gfid-req", data, 16);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to set GFID into xdata");

            GF_FREE(data);
            dict_unref(xdata);
            ida_heal_unref(ctx);

            return;
        }
        error = ida_dict_set_uint32_cow(&xdata, HEAL_KEY_FLAGS, 1);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to set heal flags into xdata");

            GF_FREE(data);
            dict_unref(xdata);
            ida_heal_unref(ctx);

            return;
        }
        error = ida_dict_set_uint64_cow(&xdata, HEAL_KEY_SIZE, ctx->size);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to set heal size into xdata");

            GF_FREE(data);
            dict_unref(xdata);
            ida_heal_unref(ctx);

            return;
        }
        ctx->heal.dst = fd_create(ctx->heal.inode, local->frame->root->pid);
        if (ctx->heal.dst == NULL)
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to create a fd");

            dict_unref(xdata);
            ida_heal_unref(ctx);

            return;
        }
        error = ida_nest_create(local, sys_bits_count64(ctx->heal.dst_mask), ctx->heal.dst_mask, ida_heal_write, &ctx->heal.loc, O_CREAT | O_TRUNC | O_RDWR, ctx->heal.mode, 0, ctx->heal.dst, xdata);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to create destinations for healing");

            ida_heal_unref(ctx);
        }
        dict_unref(xdata);
    }
    else
    {
        ida_heal_unref(ctx);
    }
}

void ida_heal_link(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    ida_inode_ctx_t * ctx;
    loc_t loc;

    gf_log(local->xl->name, GF_LOG_DEBUG, "link local: %s", local->manager.name);

    ctx = local->inode_ctx;
    if ((error == 0) && ((args->result >= 0) || (args->code == ENOENT)))
    {
        memset(&loc, 0, sizeof(loc));
        memcpy(&loc.gfid, ctx->heal.gfid, 16);
        ida_ref(local);
        error = ida_nest_link(local, sys_bits_count64(ctx->heal.dst_mask), ctx->heal.dst_mask, ida_heal_create, &loc, &ctx->heal.loc, NULL);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to link destinations for healing");

            ida_heal_unref(ctx);
        }
    }
    else
    {
        ida_heal_unref(ctx);
    }
}

void ida_heal_start(ida_local_t * local, ida_args_cbk_t * args, uintptr_t mask)
{
    ida_inode_ctx_t * ctx;
    int32_t error;
    loc_t loc;

    ctx = local->inode_ctx;
    if (__sync_lock_test_and_set(&ctx->heal.healing, 1) != 0)
    {
        gf_log(local->xl->name, GF_LOG_DEBUG, "Already healing");

        return;
    }
    local->healing = 1;
    ctx->heal.refs = 2;
    ctx->heal.error = 0;

    gf_log(local->xl->name, GF_LOG_DEBUG, "healing local: %s", local->manager.name);

    if (uuid_is_null(args->lookup.inode->gfid))
    {
        memcpy(ctx->heal.gfid, args->lookup.attr.ia_gfid, 16);
    }
    else
    {
        memcpy(ctx->heal.gfid, args->lookup.inode->gfid, 16);
    }

    ida_loc_assign(local, &ctx->heal.loc, &local->args.lookup.loc);
    ctx->heal.inode = inode_ref(args->lookup.inode);
    ctx->heal.dst_mask = mask;
    ctx->heal.src_mask = args->mixed_mask;
    ctx->heal.size = (iobpool_default_pagesize((struct iobuf_pool *)local->xl->ctx->iobuf_pool) / (16 * IDA_GF_BITS * local->private->fragments)) * (16 * IDA_GF_BITS * local->private->fragments);
    ctx->heal.mode = st_mode_from_ia(args->lookup.attr.ia_prot, args->lookup.attr.ia_type);
    gf_log(local->xl->name, GF_LOG_DEBUG, "Starting heal on nodes %lX", mask);
    ida_ref(local);
    error = ida_nest_unlink(local, sys_bits_count64(mask), mask, ida_heal_link, &local->args.lookup.loc, 0, NULL);
    if (unlikely(error != 0))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Failed to remove destinations for healing");

        ida_heal_free(ctx);
    }
    else
    {
        ctx->heal.src = fd_create(args->lookup.inode, local->frame->root->pid);
        if (ctx->heal.src == NULL)
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to create a fd");

            ida_heal_unref(ctx);

            return;
        }
        loc_copy(&loc, &local->args.lookup.loc);
        inode_unref(loc.inode);
        loc.inode = inode_ref(args->lookup.inode);
        memcpy(loc.gfid, ctx->heal.gfid, 16);
        error = ida_nest_open(local, sys_bits_count64(args->mixed_mask), args->mixed_mask, ida_heal_read, &loc, O_RDONLY, ctx->heal.src, NULL);
        loc_wipe(&loc);
        if (unlikely(error != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Failed to open sources for healing");

            ida_heal_unref(ctx);

            return;
        }
    }
}
