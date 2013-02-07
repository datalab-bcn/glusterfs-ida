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
#include "ida-manager.h"

int32_t ida_statvfs_assign(ida_local_t * local, struct statvfs * dst, struct statvfs * src)
{
    uint64_t f;

    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, src, EINVAL);
    if (src->f_frsize > src->f_bsize)
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Invalid fragment size");

        return EINVAL;
    }
    if (src->f_frsize & 511)
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Fragment size must be a multiple of 512");

        return EINVAL;
    }
    if (src->f_bsize & 511)
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Block size must be a multiple of 512");

        return EINVAL;
    }
    *dst = *src;

    f = src->f_bsize >> 9;
    dst->f_blocks *= f;
    dst->f_bfree *= f;
    dst->f_bavail *= f;

    return 0;
}

void ida_statvfs_unassign(struct statvfs * dst)
{
}

int32_t ida_statvfs_combine(ida_local_t * local, struct statvfs * dst, struct statvfs * src)
{
    if (dst->f_bsize < src->f_bsize)
    {
        dst->f_bsize = src->f_bsize;
    }
    if (dst->f_frsize < src->f_frsize)
    {
        dst->f_frsize = src->f_frsize;
    }

    if (dst->f_flag != src->f_flag)
    {
        gf_log(local->xl->name, GF_LOG_WARNING, "Filesystem flags differ: %lu - %lu", dst->f_flag, src->f_flag);
    }

    if (dst->f_blocks > src->f_blocks)
    {
        dst->f_blocks = src->f_blocks;
    }
    if (dst->f_bfree > src->f_bfree)
    {
        dst->f_bfree = src->f_bfree;
    }
    if (dst->f_bavail > src->f_bavail)
    {
        dst->f_bavail = src->f_bavail;
    }

    if (dst->f_files > src->f_files)
    {
        dst->f_files = src->f_files;
    }
    if (dst->f_ffree > src->f_ffree)
    {
        dst->f_ffree = src->f_ffree;
    }
    if (dst->f_favail > src->f_favail)
    {
        dst->f_favail = src->f_favail;
    }

    if (dst->f_namemax > src->f_namemax)
    {
        dst->f_namemax = src->f_namemax;
    }

    return 0;
}
