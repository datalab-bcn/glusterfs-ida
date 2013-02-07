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

#ifndef __IDA_XATTR_H__
#define __IDA_XATTR_H__

#include "ida-types.h"

typedef struct
{
    loc_t loc;
    fd_t * fd;
    char * name;
    dict_t * xdata;
} ida_args_getxattr_t;

typedef struct
{
    loc_t loc;
    fd_t * fd;
    dict_t * xattr;
    int32_t flags;
    dict_t * xdata;
} ida_args_setxattr_t;

typedef struct
{
    loc_t loc;
    char * name;
    dict_t * xdata;
} ida_args_removexattr_t;

typedef struct
{
    loc_t loc;
    fd_t * fd;
    gf_xattrop_flags_t flags;
    dict_t * xattr;
    int32_t lock;
    dict_t * xdata;
} ida_args_xattrop_t;

typedef struct
{
    dict_t * xattr;
    dict_t * xdata;
} ida_args_getxattr_cbk_t;

typedef struct
{
    dict_t * xdata;
} ida_args_setxattr_cbk_t;

typedef struct
{
    dict_t * xdata;
} ida_args_removexattr_cbk_t;

typedef struct
{
    dict_t * xattr;
    dict_t * xdata;
} ida_args_xattrop_cbk_t;

int32_t ida_nest_getxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, const char * name, dict_t * xdata);
int32_t ida_getxattr(call_frame_t * frame, xlator_t * this, loc_t * loc, const char * name, dict_t * xdata);

int32_t ida_nest_fgetxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, const char * name, dict_t * xdata);
int32_t ida_fgetxattr(call_frame_t * frame, xlator_t * this, fd_t * fd, const char * name, dict_t * xdata);

int32_t ida_nest_setxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, dict_t * xattr, int32_t flags, dict_t * xdata);
int32_t ida_setxattr(call_frame_t * frame, xlator_t * this, loc_t * loc, dict_t * xattr, int32_t flags, dict_t * xdata);

int32_t ida_nest_fsetxattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata);
int32_t ida_fsetxattr(call_frame_t * frame, xlator_t * this, fd_t * fd, dict_t * xattr, int32_t flags, dict_t * xdata);

int32_t ida_nest_removexattr(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, const char * name, dict_t * xdata);
int32_t ida_removexattr(call_frame_t * frame, xlator_t * this, loc_t * loc, const char * name, dict_t * xdata);

int32_t ida_nest_xattrop_common(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, inode_t * inode, loc_t * loc, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata);

int32_t ida_nest_xattrop(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, loc_t * loc, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata);
int32_t ida_xattrop(call_frame_t * frame, xlator_t * this, loc_t * loc, gf_xattrop_flags_t flags, dict_t * xattr, dict_t * xdata);

int32_t ida_nest_fxattrop(ida_local_t * local, int32_t required, uintptr_t mask, ida_callback_f callback, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, int32_t lock, dict_t * xdata);
int32_t ida_fxattrop(call_frame_t * frame, xlator_t * this, fd_t * fd, gf_xattrop_flags_t flags, dict_t * xattr, dict_t * xdata);

#endif /* __IDA_XATTR_H__ */
