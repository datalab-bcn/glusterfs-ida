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

#ifndef __IDA_LK_H__
#define __IDA_LK_H__

#include "ida-types.h"

typedef struct
{
    fd_t * fd;
    int32_t cmd;
    struct gf_flock lock;
    dict_t * xdata;
} ida_args_lk_t;

typedef struct
{
    char * volume;
    loc_t loc;
    fd_t * fd;
    int32_t cmd;
    struct gf_flock lock;
    dict_t * xdata;
} ida_args_inodelk_t;

typedef struct
{
    char * volume;
    loc_t loc;
    fd_t * fd;
    char * basename;
    entrylk_cmd cmd;
    entrylk_type type;
    dict_t * xdata;
} ida_args_entrylk_t;

typedef struct
{
    struct gf_flock lock;
    dict_t * xdata;
} ida_args_lk_cbk_t;

typedef struct
{
    dict_t * xdata;
} ida_args_inodelk_cbk_t;

typedef struct
{
    dict_t * xdata;
} ida_args_entrylk_cbk_t;

int32_t ida_nest_lk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata);
int32_t ida_lk(call_frame_t * frame, xlator_t * this, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata);

int32_t ida_nest_inodelk_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata);

int32_t ida_nest_inodelk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, loc_t * loc, int32_t cmd, struct gf_flock * lock, dict_t * xdata);
int32_t ida_inodelk(call_frame_t * frame, xlator_t * this, const char * volume, loc_t * loc, int32_t cmd, struct gf_flock * lock, dict_t * xdata);

int32_t ida_nest_finodelk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata);
int32_t ida_finodelk(call_frame_t * frame, xlator_t * this, const char * volume, fd_t * fd, int32_t cmd, struct gf_flock * lock, dict_t * xdata);

int32_t ida_nest_entrylk_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, inode_t * inode, loc_t * loc, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata);

int32_t ida_nest_entrylk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, loc_t * loc, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata);
int32_t ida_entrylk(call_frame_t * frame, xlator_t * this, const char * volume, loc_t * loc, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata);

int32_t ida_nest_fentrylk(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, const char * volume, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata);
int32_t ida_fentrylk(call_frame_t * frame, xlator_t * this, const char * volume, fd_t * fd, const char * basename, entrylk_cmd cmd, entrylk_type type, dict_t * xdata);

#endif /* __IDA_LK_H__ */
