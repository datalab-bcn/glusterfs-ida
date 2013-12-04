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

#ifndef __IDA_CHECK_H__
#define __IDA_CHECK_H__

#include <errno.h>
#include <pthread.h>

#include "logging.h"

#ifdef __GNUC__
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
#else
#define likely(x) x
#define unlikely(x) x
#endif

#define IDA_CHECK(_error, _action) \
    do \
    { \
        if ((_error) == 0) \
        { \
            _error = _action; \
        } \
    } while (0)

#define IDA_VALIDATE_ERROR(_name, _check) gf_log_callingfn(_name, GF_LOG_ERROR, "Validation failed: " #_check)

#define IDA_VALIDATE(_name, _check) \
    ({ \
        int __local_res; \
        if (unlikely(!(_check))) \
        { \
            DISPERSE_VALIDATE_ERROR(_name, _check); \
        } \
        __local_res = (_ptr) != NULL; \
        __local_res; \
    })

#define IDA_VALIDATE_OR_BREAK(_name, _check) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            break; \
        } \
    } while (0)

#define IDA_VALIDATE_OR_GOTO(_name, _check, _label) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            goto _label; \
        } \
    } while (0)

#define IDA_VALIDATE_OR_GOTO_WITH_ERROR(_name, _check, _label, _err, _value) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            _err = _value; \
            goto _label; \
        } \
    } while (0)

#define IDA_VALIDATE_OR_RETURN(_name, _check) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            return; \
        } \
    } while (0)

#define IDA_VALIDATE_OR_RETURN_NULL(_name, _check) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            return NULL; \
        } \
    } while (0)

#define IDA_VALIDATE_OR_RETURN_ERROR(_name, _check, _value) \
    do \
    { \
        if (unlikely(!(_check))) \
        { \
            IDA_VALIDATE_ERROR(_name, _check); \
            return _value; \
        } \
    } while (0)

#endif /* __IDA_CHECK_H__ */
