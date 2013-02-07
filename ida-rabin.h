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

#ifndef __IDA_RABIN_H__
#define __IDA_RABIN_H__

#include "ida-gf.h"

#define IDA_RABIN_BITS IDA_GF_BITS
#define IDA_RABIN_SIZE (1 << (IDA_RABIN_BITS))

void ida_rabin_initialize(void);
uint32_t ida_rabin_split(uint32_t size, uint32_t columns, uint32_t row, uint8_t * in, uint8_t * out);
uint32_t ida_rabin_merge(uint32_t size, uint32_t columns, uint32_t * rows, uint8_t ** in, uint8_t * out);

#endif /* __IDA_RABIN_H__ */
