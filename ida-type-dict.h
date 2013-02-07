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

#ifndef __IDA_DICT_H__
#define __IDA_DICT_H__

#include "ida-types.h"

int32_t ida_dict_assign(ida_local_t * local, dict_t ** dst, dict_t * src);
int32_t ida_dict_new(ida_local_t * local, dict_t ** dst, dict_t * src);
void ida_dict_unassign(dict_t ** dst);
int32_t ida_dict_special(char * key);
int32_t ida_dict_data_compare(data_t * dst, data_t * src);
int32_t ida_dict_equal(dict_t * dst, dict_t * src);
int32_t ida_dict_set_cow(dict_t ** dst, char * key, data_t * value);
int32_t ida_dict_set_bin_cow(dict_t ** dst, char * key, void * value, uint32_t length);
int32_t ida_dict_set_int8_cow(dict_t ** dst, char * key, int8_t value);
int32_t ida_dict_set_int16_cow(dict_t ** dst, char * key, int16_t value);
int32_t ida_dict_set_int32_cow(dict_t ** dst, char * key, int32_t value);
int32_t ida_dict_set_int64_cow(dict_t ** dst, char * key, int64_t value);
int32_t ida_dict_set_uint16_cow(dict_t ** dst, char * key, uint16_t value);
int32_t ida_dict_set_uint32_cow(dict_t ** dst, char * key, uint32_t value);
int32_t ida_dict_set_uint64_cow(dict_t ** dst, char * key, uint64_t value);
int32_t ida_dict_get_bin(dict_t * src, char * key, void ** value, uint32_t * length);
int32_t ida_dict_get_int8(dict_t * src, char * key, int8_t * value);
int32_t ida_dict_get_int16(dict_t * src, char * key, int16_t * value);
int32_t ida_dict_get_int32(dict_t * src, char * key, int32_t * value);
int32_t ida_dict_get_int64(dict_t * src, char * key, int64_t * value);
int32_t ida_dict_get_uint16(dict_t * src, char * key, uint16_t * value);
int32_t ida_dict_get_uint32(dict_t * src, char * key, uint32_t * value);
int32_t ida_dict_get_uint64(dict_t * src, char * key, uint64_t * value);
int32_t ida_dict_del_cow(dict_t ** dst, char * key);
int32_t ida_dict_clean_cow(dict_t ** dst);
int32_t ida_dict_combine_cow(ida_local_t * local, dict_t ** dst, dict_t * src);

#endif /* __IDA_DICT_H__ */
