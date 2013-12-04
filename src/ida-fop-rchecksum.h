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

#ifndef __IDA_RCHECKSUM_H__
#define __IDA_RCHECKSUM_H__

#include "ida-types.h"

typedef struct
{
    fd_t * fd;
    off_t offset;
    int32_t length;
    dict_t * xdata;
} ida_args_rchecksum_t;

typedef struct
{
    uint32_t weak;
    uint8_t * strong;
    dict_t * xdata;
} ida_args_rchecksum_cbk_t;

int32_t ida_nest_rchecksum(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, off_t offset, int32_t length, dict_t * xdata);
int32_t ida_rchecksum(call_frame_t * frame, xlator_t * this, fd_t * fd, off_t offset, int32_t length, dict_t * xdata);

#endif /* __IDA_RCHECKSUM_H__ */
