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

int32_t ida_statvfs_assign(ida_local_t * local, struct statvfs * dst, struct statvfs * src)
{
    uint64_t f;

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

bool ida_statvfs_combine(struct statvfs * dst, struct statvfs * src1,
                         struct statvfs * src2)
{
    dst->f_bsize = SYS_MAX(src1->f_bsize, src2->f_bsize);
    dst->f_frsize = SYS_MAX(src1->f_frsize, src2->f_frsize);
    dst->f_blocks = SYS_MIN(src1->f_blocks * src1->f_frsize,
                            src2->f_blocks * src2->f_frsize) / dst->f_frsize;
    dst->f_bfree = SYS_MIN(src1->f_bfree * src1->f_frsize,
                           src2->f_bfree * src2->f_frsize) / dst->f_frsize;
    dst->f_bavail = SYS_MIN(src1->f_bavail * src1->f_frsize,
                            src2->f_bavail * src2->f_frsize) / dst->f_frsize;
    dst->f_files = SYS_MAX(src1->f_files, src2->f_files);
    dst->f_ffree = SYS_MIN(src1->f_ffree, src2->f_ffree);
    dst->f_favail = SYS_MIN(src1->f_favail, src2->f_favail);
    dst->f_namemax = SYS_MIN(src1->f_namemax, src2->f_namemax);

    if (src1->f_flag != src2->f_flag)
    {
        logW("Filesystem flags differ: %lX - %lX", src1->f_flag, src2->f_flag);
    }
    dst->f_flag = src1->f_flag & src2->f_flag;

    return true;
}
