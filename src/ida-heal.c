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

#include "gfsys.h"

#include "ida-common.h"
#include "ida-mem-types.h"
#include "ida-type-dict.h"
#include "ida-type-loc.h"
#include "ida-gf.h"
#include "ida-manager.h"
#include "ida-combine.h"
#include "ida-heal.h"
#include "ida.h"

#define IDA_HEAL_FLAG_RETRY     1
#define IDA_HEAL_FLAG_DATA      2

#define IDA_HEAL_FOP(_name, _fop, _dispatcher, _req_handler, _ans_handler, \
                     _end_handler) \
    void _name##_completed(call_frame_t * frame, err_t error, \
                           ida_request_t * req, uintptr_t * data) \
    { \
        ida_answer_t * ans; \
        SYS_GF_CBK_CALL_TYPE(_fop) * args; \
        int32_t idx; \
        char buff[64]; \
        ida_heal_t * heal = frame->local; \
        logI("HEAL: " #_fop ": completed (refs=%d)", heal->refs); \
        if (!_req_handler(heal, req, data, error)) \
        { \
            ida_heal_release(heal); \
            return; \
        } \
        idx = 0; \
        list_for_each_entry(ans, &req->answers, list) \
        { \
            args = (SYS_GF_CBK_CALL_TYPE(_fop) *)((uintptr_t *)ans + \
                                                  IDA_ANS_SIZE); \
            logW("HEAL: " #_fop ": answer %u: %d(%d) %s", idx, \
                 args->op_ret, args->op_errno, to_bin(buff, sizeof(buff), \
                 ans->mask, 3)); \
            _ans_handler(heal, args->op_ret, args->op_errno, ans->mask, \
                         args); \
            idx++; \
        } \
        _end_handler(heal); \
        ida_heal_release(heal); \
    } \
    static ida_handlers_t _name##_handlers = \
    { \
        .prepare   = ida_prepare_##_fop, \
        .dispatch  = _dispatcher, \
        .completed = _name##_completed, \
        .combine   = ida_combine_##_fop, \
        .rebuild   = ida_rebuild_##_fop, \
        .copy      = ida_copy_##_fop \
    }; \
    void _name(ida_heal_t * heal, uintptr_t mask, dfc_transaction_t * txn, \
               int32_t minimum, SYS_ARGS_DECL((SYS_GF_ARGS_##_fop))) \
    { \
        char buff[64]; \
        ida_heal_acquire(heal); \
        logI("HEAL: " #_fop ": starting %s (refs=%d)", \
             to_bin(buff, sizeof(buff), mask, 3), heal->refs); \
        SYS_ASYNC( \
            ida_##_fop, (heal->frame, heal->xl, &_name##_handlers, txn, \
                         ~mask, minimum, sys_bits_count64(mask), NULL, NULL, \
                         NULL, SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop))) \
        ); \
    }

char * to_bin(char * buffer, int32_t size, uintptr_t num, int32_t digits)
{
    if (size < 1)
    {
        return NULL;
    }

    buffer += size - 1;
    *buffer = 0;

    do
    {
        if (--size <= 0)
        {
            return NULL;
        }

        *--buffer = '0' + (num & 1);
        digits--;
        num >>= 1;
    } while (num != 0);
    while (digits > 0)
    {
        if (--size <= 0)
        {
            return NULL;
        }
        *--buffer = '0';
        digits--;
    }

    return buffer;
}

void ida_heal_show_info(ida_private_t * ida, int32_t idx, ida_answer_t * ans,
                        int32_t op_ret, int32_t op_errno, struct iatt * iatt)
{
    char txt1[65], txt2[128];

    if ((iatt != NULL) && (op_ret >= 0))
    {
        sprintf(txt2, "%s %u %08o %u:%u %u:%u %lu", uuid_utoa(iatt->ia_gfid),
                iatt->ia_nlink, st_mode_from_ia(iatt->ia_prot, iatt->ia_type),
                iatt->ia_uid, iatt->ia_gid, ia_major(iatt->ia_rdev),
                ia_minor(iatt->ia_rdev), iatt->ia_size);
    }
    else
    {
        iatt = NULL;
    }
    logI("  Healing: Group %u: %s (%2d), ret=%d (errno=%d) %s", idx,
         to_bin(txt1, sizeof(txt1), ans->mask, ida->nodes), ans->count,
         op_ret, op_errno, (iatt == NULL) ? "<not available>" : txt2);
}

void ida_heal_show_msg(ida_heal_t * heal, uintptr_t mask, err_t error,
                       char * msg, ...)
{
    ida_private_t * ida;
    va_list args;
    char * buff = NULL, txt[65];

    va_start(args, msg);
    if ((vasprintf(&buff, msg, args) < 0) || (buff == NULL))
    {
        logE("Unable to format a text message");

        buff = msg;
    }
    va_end(args);

    ida = heal->xl->private;

    if (uuid_is_null(heal->loc.gfid))
    {
        logW("GFID is NULL");
    }
    if (error != 0)
    {
        logI("Healing: %s[%s]: Error %d: %s", uuid_utoa(heal->loc.gfid),
             to_bin(txt, sizeof(txt), mask, ida->nodes), error, buff);
    }
    else
    {
        logI("Healing: %s[%s]: %s", uuid_utoa(heal->loc.gfid),
             to_bin(txt, sizeof(txt), mask, ida->nodes), buff);
    }

    if (buff != msg)
    {
        free(buff);
    }
}

bool ida_default_request_handler(ida_heal_t * heal, ida_request_t * req,
                                 uintptr_t * data, err_t error)
{
    return true;
}

void ida_default_answer_handler(ida_heal_t * heal, int32_t op_ret,
                                int32_t op_errno, uintptr_t mask, void * args)
{
}

void ida_default_end_handler(ida_heal_t * heal)
{
}

void ida_heal_skip_bad(ida_heal_t * heal, int32_t op_ret, int32_t op_errno,
                       uintptr_t mask, void * data)
{
    if (op_ret < 0)
    {
        ida_heal_show_msg(heal, mask, op_errno,
                          "Unable to heal some fragments");

        atomic_and(&heal->bad, ~mask, memory_order_seq_cst);
    }
}

ida_answer_t * ida_heal_check_basic(ida_heal_t * heal, ida_request_t * req,
                                    err_t error, uintptr_t * mask)
{
    ida_private_t * ida;
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(access) * args;
    uintptr_t bad;

    SYS_TEST(
        error == 0,
        error,
        E(),
        LOG(E(), "Unable to get enough healthy information to heal the inode"),
        RETVAL(NULL)
    );

    bad = 0;
    list_for_each_entry(ans, &req->answers, list)
    {
        args = (SYS_GF_CBK_CALL_TYPE(access) *)((uintptr_t *)ans +
                                                IDA_ANS_SIZE);
        if (req->answers.next != &ans->list)
        {
            if ((args->op_ret >= 0) || (args->op_errno != ENOTCONN))
            {
                bad |= ans->mask;
            }
        }
    }

    ida = heal->xl->private;
    ans = list_entry(req->answers.next, ida_answer_t, list);
    if (ans->count < ida->fragments)
    {
        ida_heal_show_msg(heal, ans->mask, ENODATA,
                          "Insufficient quorum to heal a symlink");

        return NULL;
    }

    *mask = bad;

    return ans;
}

void ida_heal_cleanup(ida_heal_t * heal)
{
    if (heal->xdata != NULL)
    {
        sys_dict_release(heal->xdata);
        heal->xdata = NULL;
    }
    if (heal->symlink != NULL)
    {
        SYS_FREE(heal->symlink);
        heal->symlink = NULL;
    }
    if (heal->fd_src != NULL)
    {
        fd_unref(heal->fd_src);
        heal->fd_src = NULL;
    }
    if (heal->fd_dst != NULL)
    {
        fd_unref(heal->fd_dst);
        heal->fd_dst = NULL;
    }

    heal->frame->local = NULL;
    STACK_RESET(heal->frame->root);
    heal->frame->local = heal;

    heal->flags = 0;
    heal->good = 0;
    heal->bad = 0;
    heal->open = 0;
    heal->offset = 0;
}

void ida_heal_destroy(ida_heal_t * heal)
{
    SYS_CODE(
        inode_ctx_del, (heal->loc.inode, heal->xl, NULL),
        ENOENT,
        W(),
        LOG(W(), "Heal data not present on inode context")
    );

    ida_heal_cleanup(heal);
    sys_loc_release(&heal->loc);

    heal->frame->local = NULL;
    STACK_DESTROY(heal->frame->root);

    SYS_FREE(heal);
}

void ida_heal_acquire(ida_heal_t * heal)
{
    atomic_inc(&heal->refs, memory_order_seq_cst);
}

SYS_ASYNC_DECLARE(ida_heal_start, ((ida_heal_t *, heal)));

bool ida_heal_check_restart(ida_heal_t * heal, ida_request_t * req,
                            uintptr_t * data, err_t error)
{
    if (req->answers.next->next != &req->answers)
    {
        if ((heal->flags & IDA_HEAL_FLAG_RETRY) != 0)
        {
            ida_heal_start(heal);
        }
    }

    return false;
}

void ida_heal_release(ida_heal_t * heal);

IDA_HEAL_FOP(
    ida_heal_lookup_end, lookup,
    ida_dispatch_all,
    ida_heal_check_restart,
    ida_default_answer_handler,
    ida_default_end_handler
)

void ida_heal_writev_handler(ida_heal_t * heal);

IDA_HEAL_FOP(
    ida_heal_writev, writev,
    ida_dispatch_write,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_heal_writev_handler
)

void ida_heal_metadata_xattr_get(ida_heal_t * heal);

bool ida_heal_readv_handler(ida_heal_t * heal, ida_request_t * req,
                            uintptr_t * data, err_t error)
{
    ida_private_t * ida;
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(readv) * args;
    uintptr_t good, bad, mask;
    off_t offset;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    if (bad != 0)
    {
        atomic_and(&heal->good, ~bad, memory_order_seq_cst);
    }

    args = (SYS_GF_CBK_CALL_TYPE(readv) *)data;
    if (args->op_ret < 0)
    {
        ida_heal_show_msg(heal, ans->mask, args->op_errno,
                          "Unable to read healthy data");
    }
    else if (args->op_ret > 0)
    {
        offset = heal->offset;
        heal->offset += 128 * 1024;
        ida_heal_writev(heal, heal->bad, IDA_USE_DFC, 1, heal->fd_dst,
                        args->vector.iovec, args->vector.count,
                        offset, 0, args->iobref, NULL);
    }
    else
    {
        good = heal->good;
        bad = heal->bad;

        ida = heal->xl->private;
        ida_heal_cleanup(heal);
        heal->mask = good | bad;
        heal->good = good;
        heal->bad = bad;

        mask = heal->mask;
        SYS_CALL(
            dfc_begin, (ida->dfc, mask, heal->loc.inode, NULL,
                        &heal->txn),
            E(),
            LOG(E(), "Unable to initiate a transaction for healing"),
            GOTO(failed)
        );

        SYS_CALL(
            dfc_attach, (heal->txn, 0, &heal->xdata),
            E(),
            GOTO(failed_dfc)
        );

        ida_heal_metadata_xattr_get(heal);
    }

    return false;

failed_dfc:
    dfc_failed(heal->txn, sys_bits_count64(mask));
failed:

    return false;
}

IDA_HEAL_FOP(
    ida_heal_readv, readv,
    ida_dispatch_all,
    ida_heal_readv_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

void ida_heal_writev_handler(ida_heal_t * heal)
{
    ida_private_t * ida;

    if (heal->bad != 0)
    {
        ida = heal->xl->private;
        ida_heal_readv(heal, heal->good, IDA_USE_DFC, ida->fragments,
                       heal->fd_src, 128 * 1024, heal->offset, 0, NULL);
    }
}

bool ida_heal_rebuild(ida_heal_t * heal);

void ida_heal_release(ida_heal_t * heal)
{
    ida_private_t * ida;
    uintptr_t mask, bad;
    char buff[64];

    if (atomic_dec(&heal->refs, memory_order_seq_cst) == 1)
    {
        heal->frame->local = NULL;
        STACK_RESET(heal->frame->root);
        heal->frame->local = heal;

        mask = heal->mask;
        bad = heal->bad;
        if ((mask == 0) && (bad == 0))
        {
            logI("HEAL: finished");
            ida_heal_destroy(heal);
        }
        else
        {
            ida = heal->xl->private;
            if ((heal->flags & IDA_HEAL_FLAG_DATA) != 0)
            {
                logI("HEAL: recovering data"); \
                heal->flags &= ~IDA_HEAL_FLAG_DATA;
                ida_heal_readv(heal, heal->good, IDA_USE_DFC, ida->fragments,
                               heal->fd_src, 128 * 1024, heal->offset, 0,
                               NULL);
                mask = 0;
            }
            else if (bad != 0)
            {
                logI("HEAL: rebuild on %s",
                     to_bin(buff, sizeof(buff), bad, ida->nodes));
                mask &= ~(heal->good | bad);

                heal->mask &= ~mask;

                if (!ida_heal_rebuild(heal))
                {
                    logW("HEAL: rebuild failed");
                    mask |= heal->mask;
                    heal->mask = 0;
                }
            }
            else
            {
                heal->mask &= ~mask;
            }

            if (mask != 0)
            {
                logI("HEAL: finishing transaction on %s",
                     to_bin(buff, sizeof(buff), mask, ida->nodes));
                ida_heal_lookup_end(heal, mask, heal->txn, ida->fragments,
                                    &heal->loc, NULL);
            }
        }
    }
}

IDA_HEAL_FOP(
    ida_heal_unlink, unlink,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_default_end_handler
)

IDA_HEAL_FOP(
    ida_heal_rmdir, rmdir,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_default_end_handler
)

IDA_HEAL_FOP(
    ida_heal_truncate, truncate,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_default_end_handler
)

void ida_heal_remove(ida_heal_t * heal, ida_request_t * req)
{
    struct list_head * item;
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(lookup) * args;

    item = req->answers.next;
    do
    {
        item = item->next;
        ans = list_entry(item, ida_answer_t, list);
        args = (SYS_GF_CBK_CALL_TYPE(lookup) *)((uintptr_t *)ans +
                                                IDA_ANS_SIZE);

        ida_heal_show_msg(heal, ans->mask, 0, "Removing inode");

        if (args->op_ret < 0)
        {
            if ((args->op_errno == ENOENT) || (args->op_errno == ENOTDIR))
            {
                ida_heal_show_msg(heal, ans->mask, 0, "Inode already removed");
            }
            else
            {
                ida_heal_show_msg(heal, ans->mask, args->op_errno,
                                  "Don't know how to remove inode");
            }

            atomic_and(&heal->bad, ~ans->mask, memory_order_seq_cst);

            continue;
        }
        if (args->buf.ia_type == IA_IFDIR)
        {
            ida_heal_rmdir(heal, ans->mask, IDA_USE_DFC, 1, &heal->loc, 0,
                           heal->xdata);
        }
        else
        {
            ida_heal_unlink(heal, ans->mask, IDA_USE_DFC, 1, &heal->loc, 0,
                            heal->xdata);
        }
    } while (item->next != &req->answers);
}

void ida_heal_prepare(ida_heal_t * heal, ida_request_t * req)
{
    ida_private_t * ida;
    struct list_head * item;
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(lookup) * args;
    char txt1[65], txt2[65];

    ida = heal->xl->private;

    ida_heal_show_msg(heal, heal->mask, 0, "Healing from %s to %s",
                      to_bin(txt1, sizeof(txt1), heal->good, ida->nodes),
                      to_bin(txt2, sizeof(txt2), heal->bad, ida->nodes));

    item = req->answers.next;
    do
    {
        item = item->next;
        ans = list_entry(item, ida_answer_t, list);
        args = (SYS_GF_CBK_CALL_TYPE(lookup) *)((uintptr_t *)ans +
                                                IDA_ANS_SIZE);
        if ((args->op_ret < 0) ||
            (heal->iatt.ia_ino != args->buf.ia_ino) ||
            (heal->iatt.ia_type != args->buf.ia_type) ||
            (heal->iatt.ia_size != args->buf.ia_size) ||
            (uuid_compare(heal->iatt.ia_gfid, args->buf.ia_gfid) != 0))
        {
            ida_heal_show_msg(heal, ans->mask, 0, "Needs data heal");

            if (args->op_ret < 0)
            {
                if (args->op_errno != ENOENT)
                {
                    ida_heal_show_msg(heal, ans->mask, args->op_errno,
                                      "Don't know how to heal this error");

                    atomic_and(&heal->bad, ~ans->mask, memory_order_seq_cst);
                }
            }
            else
            {
                if (args->buf.ia_type == IA_IFDIR)
                {
                    ida_heal_rmdir(heal, ans->mask, IDA_USE_DFC, 1, &heal->loc,
                                   0, heal->xdata);
                }
                else if (args->buf.ia_type != IA_IFREG)
                {
                    ida_heal_unlink(heal, ans->mask, IDA_USE_DFC, 1,
                                    &heal->loc, 0, heal->xdata);
                }
                else
                {
                    atomic_or(&heal->open, ans->mask, memory_order_seq_cst);
                    ida_heal_truncate(heal, ans->mask, IDA_USE_DFC, 1,
                                      &heal->loc, 0, heal->xdata);
                }
            }
        }
        else
        {
            ida_heal_metadata_xattr_get(heal);
        }
    } while (item->next != &req->answers);
}

bool ida_heal_readlink_handler(ida_heal_t * heal, ida_request_t * req,
                               uintptr_t * data, err_t error)
{
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(readlink) * args;
    uintptr_t bad;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    args = (SYS_GF_CBK_CALL_TYPE(readlink) *)data;

    SYS_ALLOC(
        &heal->symlink, args->buf.ia_size + 1, sys_mt_uint8_t,
        E(),
        RETVAL(false)
    );
    memcpy(heal->symlink, args->path, args->buf.ia_size);
    heal->symlink[args->buf.ia_size] = 0;

    heal->good &= ans->mask;
    if (bad != 0)
    {
        atomic_or(&heal->bad, bad, memory_order_seq_cst);

        ida_heal_unlink(heal, bad, IDA_USE_DFC, 1, &heal->loc, 0, heal->xdata);
    }

    return false;
}

IDA_HEAL_FOP(
    ida_heal_readlink, readlink,
    ida_dispatch_all,
    ida_heal_readlink_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

bool ida_heal_check_state(ida_heal_t * heal, ida_request_t * req,
                          uintptr_t * data, err_t error)
{
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(lookup) * args;
    uintptr_t bad;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    if (bad == 0)
    {
        logI("Nothing to heal");

        return false;
    }

    heal->good = ans->mask;
    heal->available = ans->mask;
    heal->bad = bad;

    args = (SYS_GF_CBK_CALL_TYPE(lookup) *)data;
    if (args->op_ret < 0)
    {
        if ((args->op_errno == ENOENT) || (args->op_errno == ENOTDIR))
        {
            ida_heal_remove(heal, req);
        }
        else
        {
            ida_heal_show_msg(heal, ans->mask, args->op_errno,
                              "Don't know how to heal this error");

            atomic_and(&heal->bad, ~ans->mask, memory_order_seq_cst);
        }
    }
    else
    {
        sys_iatt_acquire(&heal->iatt, &args->buf);
        if (args->buf.ia_type == IA_IFLNK)
        {
            ida_heal_readlink(heal, heal->good, IDA_USE_DFC, 1, &heal->loc,
                              heal->iatt.ia_size, heal->xdata);
        }

        ida_heal_prepare(heal, req);
    }

    return false;
}

IDA_HEAL_FOP(
    ida_heal_lookup_start, lookup,
    ida_dispatch_all,
    ida_heal_check_state,
    ida_default_answer_handler,
    ida_default_end_handler
)

IDA_HEAL_FOP(
    ida_heal_setattr, setattr,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

void ida_heal_metadata_attr_set(ida_heal_t * heal, struct iatt * iatt)
{
    ida_heal_setattr(heal, heal->bad, IDA_USE_DFC, 1, &heal->loc, iatt,
                     GF_SET_ATTR_MODE | GF_SET_ATTR_UID | GF_SET_ATTR_GID |
                     GF_SET_ATTR_ATIME | GF_SET_ATTR_MTIME, heal->xdata);
    heal->bad = 0;
}

bool ida_heal_stat_handler(ida_heal_t * heal, ida_request_t * req,
                           uintptr_t * data, err_t error)
{
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(stat) * args;
    uintptr_t bad;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    if (bad != 0)
    {
        atomic_and(&heal->good, ~bad, memory_order_seq_cst);
        atomic_or(&heal->bad, bad, memory_order_seq_cst);
    }

    args = (SYS_GF_CBK_CALL_TYPE(stat) *)data;
    if (args->op_ret < 0)
    {
        ida_heal_show_msg(heal, ans->mask, args->op_errno,
                          "Unable to heal inode attributes");

        atomic_and(&heal->bad, ~ans->mask, memory_order_seq_cst);
    }
    else
    {
        ida_heal_metadata_attr_set(heal, &args->buf);
    }

    return false;
}

IDA_HEAL_FOP(
    ida_heal_stat, stat,
    ida_dispatch_all,
    ida_heal_stat_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

void ida_heal_metadata_attr_get(ida_heal_t * heal)
{
    if ((heal->flags & IDA_HEAL_FLAG_DATA) != 0)
    {
        ida_heal_stat(heal, heal->good, IDA_USE_DFC, 1, &heal->loc,
                      heal->xdata);
    }
    else
    {
        ida_heal_metadata_attr_set(heal, &heal->iatt);
    }
}

IDA_HEAL_FOP(
    ida_heal_setxattr, setxattr,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_heal_metadata_attr_get
)

void ida_heal_metadata_xattr_set(ida_heal_t * heal, dict_t * dict)
{
    ida_heal_setxattr(heal, heal->bad, IDA_USE_DFC, 1, &heal->loc, dict, 0,
                      heal->xdata);
}

bool ida_heal_getxattr_handler(ida_heal_t * heal, ida_request_t * req,
                               uintptr_t * data, err_t error)
{
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(getxattr) * args;
    uintptr_t bad;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    if (bad != 0)
    {
        atomic_and(&heal->good, ~bad, memory_order_seq_cst);
        atomic_or(&heal->bad, bad, memory_order_seq_cst);
    }

    args = (SYS_GF_CBK_CALL_TYPE(getxattr) *)data;
    if (args->op_ret < 0)
    {
        ida_heal_show_msg(heal, ans->mask, args->op_errno,
                          "Unable to heal inode extended attributes");

        atomic_and(&heal->bad, ~ans->mask, memory_order_seq_cst);
    }
    else
    {
        ida_heal_metadata_xattr_set(heal, args->dict);
    }

    return false;
}

IDA_HEAL_FOP(
    ida_heal_getxattr, getxattr,
    ida_dispatch_all,
    ida_heal_getxattr_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

void ida_heal_metadata_xattr_get(ida_heal_t * heal)
{
    ida_heal_getxattr(heal, heal->good, IDA_USE_DFC, 1, &heal->loc, NULL,
                      heal->xdata);
}

IDA_HEAL_FOP(
    ida_heal_symlink, symlink,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_heal_metadata_xattr_get
)

IDA_HEAL_FOP(
    ida_heal_mkdir, mkdir,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_heal_metadata_xattr_get
)

IDA_HEAL_FOP(
    ida_heal_mknod, mknod,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_skip_bad,
    ida_heal_metadata_xattr_get
)

void ida_heal_set_good(ida_heal_t * heal, int32_t op_ret, int32_t op_errno,
                       uintptr_t mask, void * data)
{
    SYS_GF_CBK_CALL_TYPE(open) * args;
    ida_fd_ctx_t * fd_ctx;
    uint64_t value;

    if (op_ret < 0)
    {
        ida_heal_show_msg(heal, mask, op_errno,
                          "Unable to reopen fd's on damaged inode fragments");
    }
    else
    {
        atomic_or(&heal->available, mask, memory_order_seq_cst);

        args = (SYS_GF_CBK_CALL_TYPE(open) *)data;

        LOCK(&args->fd->lock);

        if ((__fd_ctx_get(args->fd, heal->xl, &value) == 0) && (value != 0))
        {
            fd_ctx = (ida_fd_ctx_t *)(uintptr_t)value;
            fd_ctx->mask |= mask;
        }

        UNLOCK(&args->fd->lock);
    }
}

IDA_HEAL_FOP(
    ida_heal_open, open,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_set_good,
    ida_default_end_handler
)

bool ida_heal_open_handler(ida_heal_t * heal, ida_request_t * req,
                           uintptr_t * data, err_t error)
{
    ida_answer_t * ans;
    SYS_GF_CBK_CALL_TYPE(open) * args;
    uintptr_t bad;

    SYS_PTR(
        &ans, ida_heal_check_basic, (heal, req, error, &bad),
        ENODATA,
        E(),
        RETVAL(false)
    );

    if (bad != 0)
    {
        atomic_and(&heal->good, ~bad, memory_order_seq_cst);
        atomic_or(&heal->flags, IDA_HEAL_FLAG_RETRY, memory_order_seq_cst);
    }

    args = (SYS_GF_CBK_CALL_TYPE(open) *)data;
    if (args->op_ret < 0)
    {
        ida_heal_show_msg(heal, ans->mask, args->op_errno,
                          "Unable to open heal source files");

        heal->bad = 0;
    }

    return false;
}

void ida_heal_open_bad(ida_heal_t * heal, int32_t op_ret, int32_t op_errno,
                       uintptr_t mask, void * data)
{
    fd_t * fd;
    ida_fd_ctx_t * fd_ctx;
    uint64_t value;

    if (op_ret < 0)
    {
        ida_heal_show_msg(heal, mask, op_errno,
                          "Unable to heal some fragments");

        atomic_and(&heal->bad, ~mask, memory_order_seq_cst);
    }
    else
    {
        LOCK(&heal->loc.inode->lock);

        list_for_each_entry(fd, &heal->loc.inode->fd_list, inode_list)
        {
            if ((fd_ctx_get(fd, heal->xl, &value) == 0) && (value != 0))
            {
                fd_ctx = (ida_fd_ctx_t *)(uintptr_t)value;
                mask &= ~fd_ctx->mask;
                if (mask != 0)
                {
                    ida_heal_open(heal, mask, IDA_USE_DFC, 1, &heal->loc,
                                  fd_ctx->flags, fd, NULL);
                }
            }
        }

        UNLOCK(&heal->loc.inode->lock);
    }
}

IDA_HEAL_FOP(
    ida_heal_open_src, open,
    ida_dispatch_all,
    ida_heal_open_handler,
    ida_default_answer_handler,
    ida_default_end_handler
)

IDA_HEAL_FOP(
    ida_heal_open_dst, open,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_open_bad,
    ida_default_end_handler
)

IDA_HEAL_FOP(
    ida_heal_create, create,
    ida_dispatch_all,
    ida_default_request_handler,
    ida_heal_open_bad,
    ida_default_end_handler
)

err_t ida_heal_fd_create(xlator_t * xl, loc_t * loc, pid_t pid, fd_t ** fd)
{
    fd_t * tmp;
    err_t error;

    SYS_PTR(
        &tmp, fd_create, (loc->inode, pid),
        ENOMEM,
        E(),
        RETERR()
    );

    SYS_CALL(
        ida_fd_ctx_create, (tmp, xl, loc),
        E(),
        GOTO(failed, &error)
    );

    *fd = tmp;

    return 0;

failed:
    fd_unref(tmp);

    return error;
}

bool ida_heal_rebuild(ida_heal_t * heal)
{
    ida_private_t * ida;
    dict_t * xdata;
    uintptr_t bad, aux;

    bad = heal->bad;
    heal->bad = 0;

    if (heal->iatt.ia_type == IA_IFLNK)
    {
        ida_heal_symlink(heal, bad, IDA_USE_DFC, 1, heal->symlink, &heal->loc,
                         0, heal->xdata);
    }
    else if (heal->iatt.ia_type == IA_IFDIR)
    {
        ida_heal_mkdir(heal, bad, IDA_USE_DFC, 1, &heal->loc,
                       st_mode_from_ia(heal->iatt.ia_prot, IA_INVAL), 0,
                       heal->xdata);
    }
    else if (heal->iatt.ia_type == IA_IFREG)
    {
        SYS_CALL(
            ida_heal_fd_create, (heal->xl, &heal->loc, heal->frame->root->pid,
                                 &heal->fd_dst),
            E(),
            RETVAL(false)
        );
        SYS_CALL(
            ida_heal_fd_create, (heal->xl, &heal->loc, heal->frame->root->pid,
                                 &heal->fd_src),
            E(),
            GOTO(failed)
        );

        heal->flags |= IDA_HEAL_FLAG_DATA;
        heal->bad = bad;

        ida = heal->xl->private;

        ida_heal_open_src(heal, heal->good, heal->txn, ida->fragments,
                          &heal->loc, O_RDONLY, heal->fd_src, NULL);

        aux = heal->open & bad;
        if (aux != 0)
        {
            ida_heal_open_dst(heal, aux, heal->txn, 1, &heal->loc, O_RDWR,
                              heal->fd_dst, NULL);
            bad &= ~aux;
        }
        if (bad != 0)
        {
            xdata = NULL;
            SYS_CALL(
                sys_dict_set_uuid,(&xdata, "gfid-req", heal->loc.gfid, NULL),
                E(),
                GOTO(failed_xdata)
            );
            ida_heal_create(heal, bad, heal->txn, 1, &heal->loc, 0,
                            st_mode_from_ia(heal->iatt.ia_prot, IA_INVAL), 0,
                            heal->fd_dst, xdata);
            sys_dict_release(xdata);
        }
    }
    else
    {
        ida_heal_mknod(heal, bad, IDA_USE_DFC, 1, &heal->loc,
                       st_mode_from_ia(heal->iatt.ia_prot, IA_INVAL),
                       heal->iatt.ia_rdev, 0, heal->xdata);
    }

    return true;

failed_xdata:
    fd_unref(heal->fd_src);
    heal->fd_src = NULL;
failed:
    fd_unref(heal->fd_dst);
    heal->fd_dst = NULL;

    return false;
}

SYS_ASYNC_DEFINE(ida_heal_start, ((ida_heal_t *, heal)))
{
    xlator_t * xl;
    ida_private_t * ida;
    err_t error;

    xl = heal->xl;
    ida = xl->private;

    ida_heal_cleanup(heal);
    heal->available = heal->mask = ida->xl_up;
    heal->refs = 0;

    SYS_CALL(
        dfc_begin, (ida->dfc, heal->mask, heal->loc.inode, NULL, &heal->txn),
        E(),
        LOG(E(), "Unable to initiate a transaction for healing"),
        RETURN()
    );

    SYS_CALL(
        dfc_attach, (heal->txn, 0, &heal->xdata),
        E(),
        GOTO(failed, &error)
    );

    ida_heal_lookup_start(heal, heal->mask, IDA_USE_DFC, ida->fragments,
                          &heal->loc, heal->xdata);

    return;

failed:
    dfc_failed(heal->txn, sys_bits_count64(heal->mask));

    ida_heal_destroy(heal);
}

void ida_heal_loc(xlator_t * xl, loc_t * loc)
{
    uint64_t value;
    inode_t * inode;
    ida_heal_t * heal;
    ida_private_t * ida;

    inode = loc->inode;

    LOCK(&inode->lock);

    if ((__inode_ctx_get(inode, xl, &value) != 0) || (value == 0))
    {
        SYS_MALLOC0(
            &heal, ida_mt_ida_heal_t,
            E(),
            GOTO(failed)
        );
        heal->xl = xl;
        sys_loc_acquire(&heal->loc, loc);
        if (uuid_is_null(heal->loc.gfid))
        {
            uuid_copy(heal->loc.gfid, inode->gfid);
        }
        SYS_PTR(
            &heal->frame, create_frame, (xl, xl->ctx->pool),
            ENOMEM,
            E(),
            GOTO(failed_heal)
        );
        heal->frame->local = heal;
        ida = xl->private;
        heal->available = ida->xl_up;

        value = (uint64_t)(uintptr_t)heal;
        SYS_CODE(
            __inode_ctx_set, (inode, xl, &value),
            ENOMEM,
            E(),
            LOG(E(), "Unable to store healing information in inode context"),
            GOTO(failed_frame)
        );

        UNLOCK(&inode->lock);

        logI("Initiating self-heal");

        SYS_ASYNC(ida_heal_start, (heal));
    }
    else
    {
        UNLOCK(&inode->lock);
    }

    return;

failed_frame:
    STACK_DESTROY(heal->frame->root);
failed_heal:
    sys_loc_release(&heal->loc);
    SYS_FREE(heal);
failed:
    UNLOCK(&inode->lock);
}

void ida_heal(xlator_t * xl, loc_t * loc1, loc_t * loc2, fd_t * fd)
{
    uint64_t value;
    ida_fd_ctx_t * fd_ctx;

    if ((loc1 != NULL) && (loc1->inode != NULL))
    {
        ida_heal_loc(xl, loc1);
    }
    else if (fd != NULL)
    {
        if ((fd_ctx_get(fd, xl, &value) == 0) && (value != 0))
        {
            fd_ctx = (ida_fd_ctx_t *)(uintptr_t)value;
            ida_heal_loc(xl, &fd_ctx->loc);
        }
    }
    if ((loc2 != NULL) && (loc2->inode != NULL))
    {
        ida_heal_loc(xl, loc2);
    }
}
