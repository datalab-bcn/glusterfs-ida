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

#include "ida-check.h"
#include "ida-memory.h"
#include "ida-manager.h"

int32_t ida_buffer_add_buffer(ida_local_t * local, ida_buffer_t * buffers, uint32_t size)
{
    struct iobuf * buffer;

    buffer = iobuf_get(local->xl->ctx->iobuf_pool);
    if (unlikely(buffer == NULL))
    {
        goto failed;
    }
    if (unlikely(iobuf_pagesize(buffer) < size))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Insufficient space in buffer");

        goto failed_buffer;
    }
    if (iobref_add(buffers->buffers, buffer) != 0)
    {
        goto failed_buffer;
    }

    return 0;

failed_buffer:
    iobuf_unref(buffer);
failed:
    return ENOMEM;
}

int32_t ida_buffer_add_vector(ida_local_t * local, ida_buffer_t * buffers, uint32_t index, void * ptr, uint32_t size)
{
    struct iovec * vectors;
    uint32_t i;

    vectors = IDA_ALLOC_OR_RETURN_ERROR(local->xl->name, sizeof(struct iovec) * (buffers->count + 1), gf_common_mt_iovec, ENOMEM);
    for (i = 0; (i < buffers->count) && (i < index); i++)
    {
        vectors[i].iov_base = buffers->vectors[i].iov_base;
        vectors[i].iov_len = buffers->vectors[i].iov_len;
    }
    vectors[i].iov_base = ptr;
    vectors[i].iov_len = size;
    for (i = index + 1; i <= buffers->count; i++)
    {
        vectors[i].iov_base = buffers->vectors[i - 1].iov_base;
        vectors[i].iov_len = buffers->vectors[i - 1].iov_len;
    }
    GF_FREE(buffers->vectors);
    buffers->vectors = vectors;
    buffers->count++;

    return 0;
}

int32_t ida_buffer_new(ida_local_t * local, ida_buffer_t * dst, uint32_t size)
{
    struct iobuf * buffer;

    dst->buffers = iobref_new();
    if (unlikely(dst->buffers == NULL))
    {
        goto failed;
    }
    buffer = iobuf_get(local->xl->ctx->iobuf_pool);
    if (unlikely(buffer == NULL))
    {
        goto failed_iobref;
    }
    if (unlikely(iobuf_pagesize(buffer) < size))
    {
        gf_log(local->xl->name, GF_LOG_ERROR, "Insufficient space in buffer");

        goto failed_buffer;
    }
    if (iobref_add(dst->buffers, buffer) != 0)
    {
        goto failed_buffer;
    }
    dst->vectors = IDA_ALLOC_OR_GOTO(local->xl->name, sizeof(struct iovec), gf_common_mt_iovec, failed_buffer);
    dst->vectors->iov_base = buffer->ptr;
    dst->vectors->iov_len = size;
    dst->count = 1;

    iobuf_unref(buffer);

    return 0;

failed_buffer:
    iobuf_unref(buffer);
failed_iobref:
    iobref_unref(dst->buffers);
    dst->buffers = NULL;
failed:
    return ENOMEM;
}

int32_t ida_buffer_assign(ida_local_t * local, ida_buffer_t * dst, struct iobref * buffers, struct iovec * vectors, uint32_t count)
{
    char * ptr;
    int32_t i, error;

    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, buffers, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, count > 0, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, vectors, EINVAL);
    if (unlikely(count > 1))
    {
        error = ida_buffer_new(local, dst, iov_length(vectors, count));
        if (unlikely(error != 0))
        {
            return error;
        }

        ptr = dst->vectors->iov_base;
        for (i = 0; i < count; i++)
        {
            memcpy(ptr, vectors[i].iov_base, vectors[i].iov_len);
            ptr += vectors[i].iov_len;
        }
    }
    else
    {
        dst->buffers = iobref_ref(buffers);
        IDA_VALIDATE_OR_RETURN_ERROR(local->xl->name, dst->buffers, ENOMEM);
        dst->vectors = iov_dup(vectors, count);
        IDA_VALIDATE_OR_GOTO(local->xl->name, dst->vectors, failed);
    }

    dst->count = 1;

    return 0;

failed:
    iobref_unref(dst->buffers);
    dst->buffers = NULL;

    return -ENOMEM;
}

void ida_buffer_unassign(ida_buffer_t * dst)
{
    if (dst->vectors != NULL)
    {
        GF_FREE(dst->vectors);
        dst->vectors = NULL;
    }
    if (dst->buffers != NULL)
    {
        iobref_unref(dst->buffers);
        dst->buffers = NULL;
    }
}

int32_t ida_buffer_combine(ida_local_t * local, ida_buffer_t * dst, ida_buffer_t * src)
{
    if (iov_length(dst->vectors, dst->count) == iov_length(src->vectors, src->count))
    {
        return 0;
    }

    return EIO;
}
