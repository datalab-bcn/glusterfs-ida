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

#ifndef __IDA_MEMORY_H__
#define __IDA_MEMORY_H__

#include "ida-check.h"
#include "ida-mem-types.h"

#define IDA_ALLOC_ERROR(_name) gf_log_callingfn(_name, GF_LOG_ERROR, "Memory allocation error")

#define IDA_ALLOC_CHECK(_name, _ptr) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
        } \
    } while (0)

#define IDA_ALLOC_CHECK_OR_GOTO(_name, _ptr, _label) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
            goto _label; \
        } \
    } while (0)

#define IDA_ALLOC_CHECK_OR_GOTO_WITH_ERROR(_name, _ptr, _label, _err, _value) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
            _err = _value; \
            goto _label; \
        } \
    } while (0)

#define IDA_ALLOC_CHECK_OR_RETURN(_name, _ptr) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
            return; \
        } \
    } while (0)

#define IDA_ALLOC_CHECK_OR_RETURN_NULL(_name, _ptr) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
            return NULL; \
        } \
    } while (0)

#define IDA_ALLOC_CHECK_OR_RETURN_ERROR(_name, _ptr, _value) \
    do \
    { \
        if (unlikely((_ptr) == NULL)) \
        { \
            IDA_ALLOC_ERROR(_name); \
            return _value; \
        } \
    } while (0)

#define IDA_ALLOC(_name, _size, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_ALLOC_OR_GOTO(_name, _size, _type, _label) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK_OR_GOTO(_name, __local_ptr, _label); \
        __local_ptr; \
    })

#define IDA_ALLOC_OR_GOTO_WITH_ERROR(_name, _size, _type, _label, _err, _value) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK_OR_GOTO_WITH_ERROR(_name, __local_ptr, _label, _err, _value); \
        __local_ptr; \
    })

#define IDA_ALLOC_OR_RETURN(_name, _size, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK_OR_RETURN(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_ALLOC_OR_RETURN_NULL(_name, _size, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK_OR_RETURN_NULL(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_ALLOC_OR_RETURN_ERROR(_name, _size, _type, _value) \
    ({ \
        void * __local_ptr = GF_MALLOC(_size, _type); \
        IDA_ALLOC_CHECK_OR_RETURN_ERROR(_name, __local_ptr, _value); \
        __local_ptr; \
    })

#define IDA_MALLOC(_name, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_MALLOC_OR_GOTO(_name, _type, _label) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_GOTO(_name, __local_ptr, _label); \
        __local_ptr; \
    })

#define IDA_MALLOC_OR_GOTO_WITH_ERROR(_name, _type, _label, _err, _value) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_GOTO_WITH_ERROR(_name, __local_ptr, _label, _err, _value); \
        __local_ptr; \
    })

#define IDA_MALLOC_OR_RETURN(_name, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_MALLOC_OR_RETURN_NULL(_name, _type) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN_NULL(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_MALLOC_OR_RETURN_ERROR(_name, _type, _value) \
    ({ \
        void * __local_ptr = GF_MALLOC(sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN_ERROR(_name, __local_ptr, _value); \
        __local_ptr; \
    })

#define IDA_CALLOC(_name, _count, _type) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_CALLOC_OR_GOTO(_name, _count, _type, _label) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_GOTO(_name, __local_ptr, _label); \
        __local_ptr; \
    })

#define IDA_CALLOC_OR_GOTO_WITH_ERROR(_name, _count, _type, _label, _err, _value) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_GOTO_WITH_ERROR(_name, __local_ptr, _label, _err, _value); \
        __local_ptr; \
    })

#define IDA_CALLOC_OR_RETURN(_name, _count, _type) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_CALLOC_OR_RETURN_NULL(_name, _count, _type) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN_NULL(_name, __local_ptr); \
        __local_ptr; \
    })

#define IDA_CALLOC_OR_RETURN_ERROR(_name, _count, _type, _value) \
    ({ \
        void * __local_ptr = GF_CALLOC(_count, sizeof(_type), gf_ida_mt_##_type); \
        IDA_ALLOC_CHECK_OR_RETURN_ERROR(_name, __local_ptr, _value); \
        __local_ptr; \
    })

#endif /* __IDA_MEMORY_H__ */
