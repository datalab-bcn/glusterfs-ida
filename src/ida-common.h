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

#ifndef __IDA_COMMON_H__
#define __IDA_COMMON_H__

#include "xlator.h"

#include "ida-manager.h"

#define offset_of(_type, _field) ((uintptr_t)(&((_type *)0)->_field))
#define container_of(_ptr, _type, _field) ((_type *)(((char *)(_ptr)) - offset_of(_type, _field)))

int32_t ida_bit_count(uintptr_t mask);

off_t ida_offset_adjust(ida_local_t * local, off_t offset);
size_t ida_size_adjust(ida_local_t * local, size_t size);
void ida_offset_size_adjust(ida_local_t * loca, off_t * offset, size_t * size);

int32_t ida_xattr_update_content(ida_local_t * local, dict_t * dict, size_t * old);

int32_t ida_xattr_copy(ida_local_t * local, dict_t ** dst, dict_t * src);
int32_t ida_xattr_merge(ida_local_t * local, dict_t ** dst, dict_t * src);

#endif /* __IDA_COMMON_H__ */
