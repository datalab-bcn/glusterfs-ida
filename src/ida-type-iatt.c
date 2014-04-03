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

#include "ida-manager.h"
#include "ida-type-dict.h"
#include "ida.h"

void ida_iatt_time_merge(uint32_t * dst_sec, uint32_t * dst_nsec, uint32_t src_sec, uint32_t src_nsec)
{
    if ((*dst_sec > src_sec) || ((*dst_sec == src_sec) && (*dst_nsec > src_nsec)))
    {
        *dst_sec = src_sec;
        *dst_nsec = src_nsec;
    }
}

bool ida_iatt_combine(struct iatt * dst, struct iatt * src1,
                      struct iatt * src2)
{
    size_t blksize, blocks1, blocks2;

    if (src1->ia_blksize == src2->ia_blksize)
    {
        blksize = src1->ia_blksize;
        blocks1 = src1->ia_blocks;
        blocks2 = src2->ia_blocks;
    }
    else if (src1->ia_blksize < src2->ia_blksize)
    {
        blksize = src2->ia_blksize;
        blocks2 = src2->ia_blocks;
        blocks1 = (src1->ia_blocks * src1->ia_blksize + blksize - 1) / blksize;
    }
    else
    {
        blksize = src1->ia_blksize;
        blocks1 = src1->ia_blocks;
        blocks2 = (src2->ia_blocks * src2->ia_blksize + blksize - 1) / blksize;
    }

    if ((src1->ia_ino != src2->ia_ino) ||
//        ((src1->ia_ino != 1) && (src1->ia_nlink != src2->ia_nlink)) ||
        (src1->ia_uid != src2->ia_uid) ||
        (src1->ia_gid != src2->ia_gid) ||
        (src1->ia_rdev != src2->ia_rdev) ||
        (st_mode_from_ia(src1->ia_prot, src1->ia_type) !=
         st_mode_from_ia(src2->ia_prot, src2->ia_type)) ||
// Only check size for regular files. Directories can be identical in contents
// but have different sizes.
        ((src1->ia_type == IA_IFREG) && (src1->ia_size != src2->ia_size)) ||
        (uuid_compare(src1->ia_gfid, src2->ia_gfid) != 0))
    {
        logI("IDA: iatt combine failed (%lu, %u, %u, %u, %lu, %lu, %X), "
             "(%lu, %u, %u, %u, %lu, %lu, %X)",
             src1->ia_ino, src1->ia_nlink, src1->ia_uid, src1->ia_gid,
             src1->ia_rdev, src1->ia_size, st_mode_from_ia(src1->ia_prot, src1->ia_type),
             src2->ia_ino, src2->ia_nlink, src2->ia_uid, src2->ia_gid,
             src2->ia_rdev, src2->ia_size, st_mode_from_ia(src2->ia_prot, src2->ia_type));
        return false;
    }

    *dst = *src1;

    dst->ia_blksize = blksize;
    dst->ia_blocks = blocks1 + blocks2;

    ida_iatt_time_merge(&dst->ia_atime, &dst->ia_atime_nsec, src2->ia_atime,
                        src2->ia_atime_nsec);
    ida_iatt_time_merge(&dst->ia_mtime, &dst->ia_mtime_nsec, src2->ia_mtime,
                        src2->ia_mtime_nsec);
    ida_iatt_time_merge(&dst->ia_ctime, &dst->ia_ctime_nsec, src2->ia_ctime,
                        src2->ia_ctime_nsec);

    return true;
}
/*
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
*/

void ida_iatt_rebuild(ida_private_t * ida, struct iatt * iatt, int32_t count)
{
    iatt->ia_blocks = (iatt->ia_blocks * ida->fragments + count - 1) / count;
}
