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

#include "ida-memory.h"
#include "ida-common.h"
#include "ida-type-dict.h"
#include "ida-manager.h"
#include "ida.h"

static void ida_rebuild(ida_local_t * local, ida_args_cbk_t * group);

static void ida_pending_inc(ida_local_t * local)
{
    __sync_fetch_and_add(&local->pending, 1);
}

/*
static char * ida_to_bin(uintptr_t mask, uint32_t digits, char * buffer)
{
    int32_t i;

    if (digits > 64)
    {
        *(int *)0 = 0;
    }

    for (i = 0; i < digits; i++)
    {
        buffer[i] = '0' + ((mask >> (digits - i - 1)) & 1);
    }
    buffer[digits] = 0;

    return buffer;
}
*/

static void ida_pending_dec(ida_local_t * local)
{
    uintptr_t mask;
    ida_args_cbk_t * args;
    int32_t error;
//    char buff[65];

    if (__sync_fetch_and_sub(&local->pending, 1) == 1)
    {
        if (!local->reported)
        {
            error = (local->error != 0) ? local->error : EIO;
            if (list_empty(&local->args_groups))
            {
                local->callback(local, 0, error, NULL);
            }
            else
            {
                args = list_entry(local->args_groups.next, ida_args_cbk_t, list);
                mask = args->mixed_mask;

                if ((args->mixed_count < local->private->fragments) && !local->healing)
                {
                    gf_log(local->xl->name, GF_LOG_DEBUG, "Failure from '%s'", local->manager.name);

                    local->callback(local, args->mixed_mask, error, args);
                }
                else
                {
                    LOCK(&local->lock);
                    ida_rebuild(local, args);
                    UNLOCK(&local->lock);
                    if (!local->reported)
                    {
                        local->callback(local, args->mixed_mask, EIO, args);
                    }
                }
            }
//            gf_log(local->xl->name, GF_LOG_DEBUG, "before %s callback: local=%p, private=%p, nodes=%u, mask=%s", local->manager.name, local, local->private, local->private->nodes, ida_to_bin(args->mixed_mask, local->private->nodes, buff));
//            gf_log(local->xl->name, GF_LOG_DEBUG, "after %s callback: local=%p, private=%p, nodes=%u, mask=%s", local->manager.name, local, local->private, local->private->nodes, ida_to_bin(args->mixed_mask, local->private->nodes, buff));
        }

//        ida_args_cbk_for_each_entry(args, local)
//        {
//            gf_log(local->xl->name, GF_LOG_DEBUG, "(%s) group: %u nodes (%s) (result=%d, code=%d)", local->manager.name, args->mixed_count, ida_to_bin(args->mixed_mask, local->private->nodes, buff), args->result, args->code);
//        }

        if (local->manager.finish != NULL)
        {
            local->manager.finish(local);
        }
        else
        {
            ida_unref(local);
        }
    }
}

int32_t ida_inode_context(ida_local_t * local, inode_t * inode)
{
    uint64_t value;
    ida_inode_ctx_t * context;
    int32_t error;

    LOCK(&inode->lock);

    if (unlikely(__inode_ctx_get(inode, local->xl, &value) != 0) || unlikely(value == 0))
    {
        context = IDA_MALLOC_OR_GOTO_WITH_ERROR(local->xl->name, ida_inode_ctx_t, done, error, ENOMEM);
        memset(context, 0,sizeof(ida_inode_ctx_t));
        value = (uint64_t)(uintptr_t)context;
        if (unlikely(__inode_ctx_set(inode, local->xl, &value) != 0))
        {
            gf_log(local->xl->name, GF_LOG_ERROR, "Unable to set inode context");

            GF_FREE(context);
            error = EIO;

            goto done;
        }
    }
    else
    {
        context = (ida_inode_ctx_t *)(uintptr_t)value;
    }

    local->inode = inode;
    local->inode_ctx = context;

    error = 0;

done:
    UNLOCK(&inode->lock);

    return error;
}

int32_t ida_nest(ida_local_t ** local, xlator_t * xl, call_frame_t * base, inode_t * inode, ida_manager_t * manager, int32_t required, uintptr_t mask, ida_callback_f callback)
{
    ida_private_t * priv;
    call_frame_t * frame;
    ida_local_t * link;
    size_t size;
    int32_t error;

    IDA_VALIDATE_OR_RETURN_ERROR("ida", xl, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(xl->name, xl->private, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(xl->name, base, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(xl->name, manager, EINVAL);
    IDA_VALIDATE_OR_RETURN_ERROR(xl->name, callback, EINVAL);

//    gf_log(xl->name, GF_LOG_DEBUG, "Function call: '%s'", manager->name);

    priv = xl->private;
    size = sizeof(ida_local_t) + sizeof(ida_args_cbk_t) * priv->nodes;
    link = IDA_ALLOC_OR_RETURN_ERROR(xl->name, size, gf_ida_mt_ida_local_t, ENOMEM);
    memset(link, 0, size);

    frame = copy_frame(base);
    if (unlikely(frame == NULL))
    {
        gf_log(xl->name, GF_LOG_ERROR, "Unable to create a new stack frame");

        GF_FREE(link);

        return ENOMEM;
    }

    LOCK_INIT(&link->lock);

    link->xl = xl;
    link->private = priv;
    link->frame = frame;
    link->next = base;
    link->refs = 1;
    link->pending = 1;
    link->required = required;

    if (base->local != NULL)
    {
        link->healing = ((ida_local_t *)base->local)->healing;
    }

    link->mask = priv->xl_up;
    if (mask != 0)
    {
        link->mask &= mask;
    }

    link->callback = callback;
    INIT_LIST_HEAD(&link->args_groups);

    memcpy(&link->manager, manager, sizeof(ida_manager_t));

    if (inode != NULL)
    {
        error = ida_inode_context(link, inode);
        if (unlikely(error != 0))
        {
            FRAME_DESTROY(frame);
            GF_FREE(link);

            return error;
        }
    }
    else
    {
        gf_log(xl->name, GF_LOG_WARNING, "nest without inode for %s", manager->name);
    }

    frame->local = link;

    *local = link;

    return 0;
}

void ida_ref(ida_local_t * local)
{
    __sync_fetch_and_add(&local->refs, 1);
}

void ida_unref(ida_local_t * local)
{
    call_frame_t * frame;

    if (__sync_fetch_and_sub(&local->refs, 1) == 1)
    {
        if (local->manager.wipe != NULL)
        {
            local->manager.wipe(local);
        }

        frame = local->frame;
        frame->local = NULL;
        GF_FREE(local);

        STACK_DESTROY(frame->root);
    }
}

int32_t ida_dispatch(ida_local_t * local, int32_t index, ida_args_t * args)
{
    int32_t error;

    if (unlikely((__sync_fetch_and_and(&local->completed_mask, ~(1ULL << index)) & (1ULL << index)) == 0))
    {
        gf_log(local->xl->name, GF_LOG_WARNING, "Partial answer from child '%s' has not been received", local->private->xl_list[index]->name);
    }

    return local->manager.dispatch(local, local->private->xl_list[index], local->frame, index, args);
}

void ida_execute(ida_local_t * local)
{
    xlator_t ** xls;
    call_frame_t * frame;
    uintptr_t mask;
    int32_t index, childs, error;

    frame = local->frame;
    xls = local->private->xl_list;

    childs = ida_bit_count(local->mask);
    if (local->required == IDA_EXECUTE_MAX)
    {
        local->required = childs;
    }
    else if (local->required <= 0)
    {
        local->required = local->private->fragments - local->required;
    }
    local->succeeded_mask = 0;
    local->completed_mask = 0;
    local->code = 0;
    local->succeeded = 0;
    local->reported = 0;

    error = EIO;
    mask = local->mask;
    if (local->required <= childs)
    {
        index = 0;
        while (mask != 0)
        {
            if ((mask & 1) != 0)
            {
                ida_pending_inc(local);
                error = local->manager.dispatch(local, xls[index], frame, index, &local->args);
                if (unlikely(error != 0))
                {
                    ida_complete(local, error);
                    error = 0;
                }
            }
            index++;
            mask >>= 1;
        }
    }

    ida_complete(local, error);
}

void ida_report(ida_local_t * local, uintptr_t mask, int32_t error, ida_args_cbk_t * args)
{
    int32_t childs;

    if (mask == 0)
    {
        mask = local->mask;
    }
    childs = ida_bit_count(mask);
    local->required = childs;
    local->succeeded_mask = mask;
    local->completed_mask = mask;
    local->code = 0;
    local->succeeded = childs;
    local->reported = 1;

    local->callback(local, mask, error, args);
}

ida_local_t * ida_process(ida_args_cbk_t ** args, xlator_t * xl, call_frame_t * frame, void * cookie, int32_t result, int32_t code, dict_t * xdata)
{
    ida_private_t * priv;
    ida_local_t * local;
    uint32_t index;

    IDA_VALIDATE_OR_RETURN_NULL("ida", xl);
    IDA_VALIDATE_OR_RETURN_NULL(xl->name, xl->private);
    IDA_VALIDATE_OR_RETURN_NULL(xl->name, frame);
    IDA_VALIDATE_OR_RETURN_NULL(xl->name, frame->local);
//    IDA_VALIDATE_OR_RETURN_NULL(xl->name, xdata);

    local = frame->local;

    IDA_VALIDATE_OR_RETURN_NULL(xl->name, local->private == xl->private);
    IDA_VALIDATE_OR_RETURN_NULL(xl->name, local->frame == frame);

    priv = local->private;
    index = (uint32_t)(uintptr_t)cookie;

    IDA_VALIDATE_OR_RETURN_NULL(xl->name, index < priv->nodes);

    if (unlikely(__sync_fetch_and_or(&local->completed_mask, 1ULL << index) & (1ULL << index)))
    {
        gf_log(xl->name, GF_LOG_WARNING, "Duplicate answer from child '%s'", priv->xl_list[index]->name);

        return NULL;
    }

    local->args_cbk[index].index = index;
    local->args_cbk[index].result = result;
    local->args_cbk[index].code = code;
    INIT_LIST_HEAD(&local->args_cbk[index].list);
    local->args_cbk[index].mixed_count = 1;
    local->args_cbk[index].mixed_mask = 1ULL << index;

/*
    if (unlikely(ida_dict_get_uint64(xdata, HEAL_KEY_AVAILABLE, &local->args_cbk[index].heal_available) != 0))
    {
        gf_log(xl->name, GF_LOG_ERROR, "Heal info not available");

        return NULL;
    }
*/
    *args = &local->args_cbk[index];

    return local;
}

static void ida_rebuild(ida_local_t * local, ida_args_cbk_t * group)
{
    uintptr_t mask, limit, skip, subset, refill[65];
    uint32_t i, bits;
    int32_t error;

    local->reported = 1;

    do
    {
        mask = group->mixed_mask;

        UNLOCK(&local->lock);

        bits = local->required;
        limit = (1ULL << ida_bit_count(mask)) - 1;
        skip = (~mask) & limit;
        refill[bits] = 0;
        for (i = 0; bits > 0; i++)
        {
            if (mask & (1ULL << i))
            {
                refill[bits - 1] = refill[bits] | (1ULL << i);
                bits--;
            }
        }

        subset = 0;
        do
        {
            subset |= refill[ida_bit_count(subset)];

            error = 0;
            if (local->manager.rebuild != NULL)
            {
                error = local->manager.rebuild(local, subset, group);
            }
            if (likely(error <= 0))
            {
                if (error == 0)
                {
                    local->cbk = group;

//                    gf_log(local->xl->name, GF_LOG_DEBUG, "Completed '%s' (nodes=%u, result=%d, code=%d)", local->manager.name, group->mixed_count, group->result, group->code);

                    local->callback(local, subset, 0, group);

                    LOCK(&local->lock);

                    return;
                }

                break;
            }

            subset += (subset & (-subset)) + skip;
            subset &= mask;
        } while (subset != 0);

        LOCK(&local->lock);
    } while (mask != group->mixed_mask);

    local->reported = 0;
}

static void ida_combine_group(ida_local_t * local, ida_args_cbk_t * args)
{
    struct list_head * item;
    ida_args_cbk_t * group;

    group = args;
    ida_args_cbk_for_each(item, local)
    {
        group = container_of(item, ida_args_cbk_t, list);
        if (unlikely(group->mixed_count == 0))
        {
            break;
        }
        if (local->manager.combine(local, group, args) == 0)
        {
            group->mixed_count++;
            args->mixed_count = 0;
            group->mixed_mask |= args->mixed_mask;
            args->mixed_mask = 0;

            while (item->prev != &local->args_groups)
            {
                item = item->prev;
                if (container_of(item, ida_args_cbk_t, list)->mixed_count < group->mixed_count)
                {
                    list_move_tail(&group->list, item);

                    break;
                }
            }

            item = &local->args_groups;

            break;
        }
    }
    list_add_tail(&args->list, item);

    if (!local->reported && (group->mixed_count >= local->required))
    {
        ida_rebuild(local, group);
    }
}

void ida_combine(ida_local_t * local, ida_args_cbk_t * args, int32_t error)
{
    LOCK(&local->lock);

    if (args->result >= 0)
    {
        local->succeeded_mask |= args->mixed_mask;
        local->succeeded++;
    }

    if (unlikely(error != 0) || local->reported)
    {
        args->mixed_count = 0;
        args->mixed_mask = 0;
        list_add_tail(&args->list, &local->args_groups);
    }
    else
    {
        ida_combine_group(local, args);
    }

    UNLOCK(&local->lock);

    ida_complete(local, error);
}

void ida_complete(ida_local_t * local, int32_t error)
{
    if (error != 0)
    {
        __sync_val_compare_and_swap(&local->error, 0, error);
    }

    ida_pending_dec(local);
}
