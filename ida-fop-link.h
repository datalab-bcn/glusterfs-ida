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

#ifndef __IDA_LINK_H__
#define __IDA_LINK_H__

#include "ida-types.h"

typedef struct
{
    loc_t old_loc;
    loc_t new_loc;
    int32_t unlock;
    int32_t unlock2;
    dict_t * xdata;
} ida_args_link_t;

typedef struct
{
    inode_t * inode;
    struct iatt attr;
    struct iatt attr_ppre;
    struct iatt attr_ppost;
    dict_t * xdata;
} ida_args_link_cbk_t;

int32_t ida_nest_link(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * old_loc, loc_t * new_loc, dict_t * xdata);
int32_t ida_link(call_frame_t * frame, xlator_t * this, loc_t * old_loc, loc_t * new_loc, dict_t * xdata);

#endif /* __IDA_LINK_H__ */
