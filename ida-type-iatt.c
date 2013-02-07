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

#include "ida-check.h"
#include "ida-memory.h"
#include "ida-manager.h"
#include "ida-type-dict.h"
#include "ida.h"

int32_t ida_iatt_assign(ida_local_t * local, struct iatt * dst, struct iatt * src)
{
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, src, EINVAL);
    *dst = *src;
    if (dst->ia_type != IA_INVAL)
    {
        if (dst->ia_blksize < 4096)
        {
            dst->ia_blksize = 4096;
        }
        dst->ia_dev = local->private->device;
    }

    return 0;
}

void ida_iatt_unassign(struct iatt * dst)
{
}

void ida_iatt_time_merge(uint32_t * dst_sec, uint32_t * dst_nsec, uint32_t src_sec, uint32_t src_nsec)
{
    if ((*dst_sec > src_sec) || ((*dst_sec == src_sec) && (*dst_nsec > src_nsec)))
    {
        *dst_sec = src_sec;
        *dst_nsec = src_nsec;
    }
}

int32_t ida_iatt_combine(ida_local_t * local, struct iatt * dst, struct iatt * src)
{
    if (/*(dst->ia_ino != src->ia_ino) ||*/
        (uuid_compare(dst->ia_gfid, src->ia_gfid) != 0) ||
        (st_mode_from_ia(dst->ia_prot, dst->ia_type) != st_mode_from_ia(src->ia_prot, src->ia_type)) ||
//        (dst->ia_nlink != src->ia_nlink) ||
        (dst->ia_uid != src->ia_uid) ||
        (dst->ia_gid != src->ia_gid) ||
        (dst->ia_rdev != src->ia_rdev) ||
        (dst->ia_size != src->ia_size) ||
        (dst->ia_blocks != src->ia_blocks))
    {
        return EIO;
    }

    if (dst->ia_blksize < src->ia_blksize)
    {
        dst->ia_blksize = src->ia_blksize;
    }

    ida_iatt_time_merge(&dst->ia_atime, &dst->ia_atime_nsec, src->ia_atime, src->ia_atime_nsec);
    ida_iatt_time_merge(&dst->ia_mtime, &dst->ia_mtime_nsec, src->ia_mtime, src->ia_mtime_nsec);
    ida_iatt_time_merge(&dst->ia_ctime, &dst->ia_ctime_nsec, src->ia_ctime, src->ia_ctime_nsec);

    return 0;
}

void ida_iatt_adjust(ida_local_t * local, struct iatt * dst, dict_t * xattr, inode_t * inode)
{
    int32_t error;
    size_t real_size, size;
    uint64_t ctx_value;
    ida_inode_ctx_t * ctx;

    if (dst->ia_type == IA_IFREG)
    {
        if (inode != NULL)
        {
            if ((inode_ctx_get(inode, local->xl, &ctx_value) != 0) || (ctx_value == 0))
            {
                ctx = IDA_MALLOC(local->xl->name, ida_inode_ctx_t);
                if (ctx != NULL)
                {
                    if (unlikely(inode_ctx_set(inode, local->xl, (uint64_t *)&ctx) != 0))
                    {
                        GF_FREE(ctx);
                        ctx = NULL;

                        gf_log(local->xl->name, GF_LOG_ERROR, "Cannot set inode context");
                    }
                }
            }
            else
            {
                ctx = (ida_inode_ctx_t *)ctx_value;
            }
        }

        size = dst->ia_size * local->private->fragments;

        if (xattr != NULL)
        {
            error = ida_dict_get_uint64(xattr, IDA_KEY_SIZE, &real_size);
            if (unlikely(error != 0))
            {
                gf_log(local->xl->name, GF_LOG_ERROR, "File has an undefined size");
            }
            else if (unlikely(real_size > size))
            {
                gf_log(local->xl->name, GF_LOG_ERROR, "File is truncated");
            }
            else
            {
                size = real_size;
            }
            if (ctx != NULL)
            {
                ctx->size = real_size;
            }
            dict_del(xattr, IDA_KEY_SIZE);
        }

        dst->ia_size = size;

        if (ctx != NULL)
        {
            LOCK(&local->inode->lock);

            if (ctx->size != dst->ia_size)
            {
                if (dst->ia_size > ctx->size)
                {
                    dst->ia_size = ctx->size;
                }
                else
                {
                    ctx->size = dst->ia_size;

                    gf_log(local->xl->name, GF_LOG_WARNING, "Inconsistent size for inode");
                }
            }

            UNLOCK(&local->inode->lock);
        }

        dst->ia_blocks *= local->private->fragments;
    }
}
