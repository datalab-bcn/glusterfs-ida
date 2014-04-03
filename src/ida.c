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
#include "gfdfc.h"

#include <ctype.h>
#include <sys/uio.h>

#include "ida-common.h"
#include "ida-mem-types.h"
#include "ida-rabin.h"
#include "ida-manager.h"
#include "ida-combine.h"
#include "ida.h"

#define IDA_MAX_NODES 24
#define IDA_MAX_FRAGMENTS 15

err_t ida_parse_size(xlator_t * this, uint32_t * nodes, uint32_t * redundancy)
{
    char * size;
    char ** tokens;
    int32_t i, token_count;
    err_t error;

    error = 0;

    size = NULL;
    GF_OPTION_INIT("size", size, str, done);
    SYS_TEST(
        size != NULL,
        ENOENT,
        E(),
        LOG(E(), "Option 'size' not defined."),
        GOTO(done, &error)
    );
    SYS_CODE(
        gf_strsplit, (size, ":", &tokens, &token_count),
        EINVAL,
        E(),
        LOG(E(), "Unable to parse option 'size'."),
        GOTO(done, &error)
    );
    SYS_TEST(
        token_count == 2,
        EINVAL,
        E(),
        LOG(E(), "Invalid format of option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_CODE(
        gf_string2uint32, (tokens[0], nodes),
        EINVAL,
        E(),
        LOG(E(), "The number of nodes is invalid in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_CODE(
        gf_string2uint32, (tokens[1], redundancy),
        EINVAL,
        E(),
        LOG(E(), "The level of redundancy is invalid in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *redundancy < *nodes,
        EINVAL,
        E(),
        LOG(E(), "Invalid redundancy value in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *nodes <= IDA_MAX_NODES,
        EINVAL,
        E(),
        LOG(E(), "Too many nodes specified in option 'size'."),
        GOTO(free_tokens, &error)
    );
    SYS_TEST(
        *nodes - *redundancy <= IDA_MAX_FRAGMENTS,
        EINVAL,
        E(),
        LOG(E(), "Unsupported combination of nodes and redundancy in option "
                 "'size'."),
        GOTO(free_tokens, &error)
    );

free_tokens:
    for (i = 0; i < token_count; i++)
    {
        SYS_FREE(tokens[i]);
    }
    SYS_FREE(tokens);
done:
    return error;
}

err_t ida_parse_options(xlator_t * this)
{
    ida_private_t * priv;

    priv = this->private;

    SYS_CALL(
        ida_parse_size, (this, &priv->nodes, &priv->redundancy),
        E(),
        RETERR()
    );

    priv->fragments = priv->nodes - priv->redundancy;
    SYS_TEST(
        priv->redundancy < priv->fragments,
        EINVAL,
        E(),
        LOG(E(), "Cannot use a redundancy greater or equal than 100%% of the "
                 "effective space. Use replicated volume instead."),
        RETERR()
    );

    priv->node_mask = (1ULL << priv->nodes) - 1ULL;
    priv->block_size = priv->fragments * IDA_GF_BITS * 16;

    return 0;
}

err_t ida_prepare_childs(xlator_t * this)
{
    ida_private_t * priv;
    xlator_list_t * child;
    int32_t count;

    priv = this->private;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        count++;
    }
    SYS_TEST(
        count >= 3,
        EINVAL,
        E(),
        LOG(E(), "A dispersed volume must have three subvolumes at least."),
        RETERR()
    );
    SYS_TEST(
        count == priv->nodes,
        EINVAL,
        E(),
        LOG(E(), "The number of subvolumes does not match the number of "
                 "configured nodes."),
        RETERR()
    );

    SYS_CALLOC0(
        &priv->xl_list, count, ida_mt_xlator_t,
        E(),
        RETERR()
    );
    priv->xl_up = 0;

    count = 0;
    for (child = this->children; child != NULL; child = child->next)
    {
        priv->xl_list[count++] = child->xlator;
    }

    return 0;
}

void __ida_destroy_private(xlator_t * this)
{
    ida_private_t * priv;

    priv = this->private;
    if (priv != NULL)
    {
        if (priv->xl_list != NULL)
        {
            SYS_FREE(priv->xl_list);
            priv->xl_list = NULL;
        }

        sys_mutex_terminate(&priv->lock);

        SYS_FREE(priv);
        this->private = NULL;
    }
}

int32_t mem_acct_init(xlator_t * this)
{
    SYS_CODE(
        xlator_mem_acct_init, (this, ida_mt_end + 1),
        ENOMEM,
        E(),
        LOG(E(), "Memory accounting initialization failed."),
        RETVAL(-1)
    );

    return 0;
}

int32_t reconfigure(xlator_t * this, dict_t * options)
{
    logE("Online volume reconfiguration is not supported.");

    return -1;
}

int32_t notify(xlator_t * this, int32_t event, void * data, ...)
{
    ida_private_t * priv;

    priv = this->private;

    return dfc_default_notify(priv->dfc, this, event, data);
}

SYS_DELAY_CREATE(ida_up, ((xlator_t *, xl)))
{
    ida_private_t * priv;

    priv = xl->private;

    sys_mutex_lock(&priv->lock);

    if (priv->delay != NULL)
    {
        sys_delay_release(priv->delay);
        priv->delay = NULL;
    }

    if (!priv->up && (sys_bits_count64(priv->xl_up) >= priv->fragments))
    {
        priv->up = true;
        logI("Going UP");
        default_notify(xl, GF_EVENT_CHILD_UP, NULL);
    }

    sys_mutex_unlock(&priv->lock);
}

void dfc_notify(dfc_t * dfc, xlator_t * xl, int32_t event)
{
    ida_private_t * priv;
    uintptr_t * delay;
    int32_t i;

    priv = dfc->xl->private;

    sys_mutex_lock(&priv->lock);

    switch (event)
    {
        case DFC_CHILD_UP:
            logI("Event CHILD_UP: %s", xl->name);

            for (i = 0; i < priv->nodes; i++)
            {
                if (xl == priv->xl_list[i])
                {
                    priv->xl_up |= 1ULL << i;
                }
            }

            i = sys_bits_count64(priv->xl_up);
            if (i == priv->fragments)
            {
                priv->delay = SYS_DELAY(1000, ida_up, (dfc->xl), 1);
            }
            else if ((i == priv->nodes) && (priv->delay != NULL))
            {
                delay = priv->delay;
                priv->delay = NULL;
                sys_delay_execute(delay, 0);
            }

            break;

        case DFC_CHILD_DOWN:
            logI("Event CHILD_DOWN: %s", xl->name);

            for (i = 0; i < priv->nodes; i++)
            {
                if (xl == priv->xl_list[i])
                {
                    priv->xl_up &= ~(1ULL << i);
                }
            }

            if (sys_bits_count64(priv->xl_up) == priv->fragments - 1)
            {
                delay = priv->delay;
                if (delay != NULL)
                {
                    priv->delay = NULL;
                    sys_delay_cancel(delay, false);
                }
                if (priv->up)
                {
                    priv->up = false;
                    logI("Going DOWN");
                    default_notify(dfc->xl, GF_EVENT_CHILD_DOWN, NULL);
                }
            }

            break;
    }

    sys_mutex_unlock(&priv->lock);
}

int32_t init(xlator_t * this)
{
    ida_private_t * priv;

    if (this->parents == NULL)
    {
        logW("Volume does not have parents.");
    }

    SYS_MALLOC0(
        &priv, ida_mt_ida_private_t,
        E(),
        RETVAL(-1)
    );

    sys_mutex_initialize(&priv->lock);

    priv->xl = this;

    this->private = priv;

    SYS_CALL(
        ida_parse_options, (this),
        E(),
        GOTO(failed)
    );
    SYS_CALL(
        ida_prepare_childs, (this),
        E(),
        GOTO(failed)
    );

    ida_rabin_initialize();

    SYS_CALL(
        gfsys_initialize, (NULL, false),
        E(),
        GOTO(failed)
    );

    SYS_CALL(
        dfc_initialize, (this, 64, 16, dfc_notify, &priv->dfc),
        E(),
        GOTO(failed)
    );

    logD("Disperse translator loaded.");

    return 0;

failed:
    __ida_destroy_private(this);

    return -1;
}

void fini(xlator_t * this)
{
    __ida_destroy_private(this);
}

#define IDA_FOP_MODE_DIO IDA_SKIP_DFC
#define IDA_FOP_MODE_DFC IDA_USE_DFC

#define IDA_FOP_COUNT_INC 0
#define IDA_FOP_COUNT_MIN 1
#define IDA_FOP_COUNT_ALL 2
#define IDA_FOP_COUNT_MOD 2

#define IDA_FOP_REQUIRE_ONE 1
#define IDA_FOP_REQUIRE_MIN 2
#define IDA_FOP_REQUIRE_ALL 3

#define IDA_FOP(_fop) \
    SYS_ASYNC_DEFINE(ida_##_fop, ((call_frame_t *, frame), \
                                  (xlator_t *, this), \
                                  (ida_handlers_t *, handlers), \
                                  (dfc_transaction_t *, txn), \
                                  (uintptr_t, bad), \
                                  (int32_t, minimum), \
                                  (int32_t, required), \
                                  (loc_t, loc1, PTR, sys_loc_acquire, \
                                                     sys_loc_release), \
                                  (loc_t, loc2, PTR, sys_loc_acquire, \
                                                     sys_loc_release), \
                                  (fd_t *, fd1, COPY, sys_fd_acquire, \
                                                      sys_fd_release), \
                                  SYS_GF_ARGS_##_fop)) \
    { \
        ida_private_t * ida; \
        ida_request_t * req; \
        SYS_GF_FOP_CALL_TYPE(_fop) * args; \
        ida = this->private; \
        req = (ida_request_t *)SYS_GF_FOP_CALL(_fop, IDA_REQ_SIZE); \
        args = (SYS_GF_FOP_CALL_TYPE(_fop) *)((uintptr_t *)req + \
                                              IDA_REQ_SIZE); \
        req->frame = frame; \
        SYS_PTR( \
            &req->rframe, copy_frame, (frame), \
            ENOMEM, \
            E(), \
            GOTO(failed) \
        ); \
        req->xl = this; \
        req->handlers = handlers; \
        req->txn = txn; \
        req->last_sent = 0; \
        req->sent = 0; \
        req->failed = 0; \
        req->completed = 0; \
        req->bad = bad; \
        req->xdata = &args->xdata; \
        sys_loc_acquire(&req->loc1, loc1); \
        sys_loc_acquire(&req->loc2, loc2); \
        sys_fd_acquire(&req->fd, fd1); \
        sys_lock_initialize(&req->lock); \
        INIT_LIST_HEAD(&req->answers); \
        req->minimum = minimum; \
        req->required = required; \
        req->pending = 0; \
        if (handlers->prepare(ida, req)) \
        { \
            handlers->dispatch(ida, req); \
            return; \
        } \
    failed: \
        handlers->completed(frame, EIO, req, NULL); \
        sys_gf_args_free((uintptr_t *)req); \
    }

IDA_FOP(access)
IDA_FOP(create)
IDA_FOP(entrylk)
IDA_FOP(fentrylk)
IDA_FOP(flush)
IDA_FOP(fsync)
IDA_FOP(fsyncdir)
IDA_FOP(getxattr)
IDA_FOP(fgetxattr)
IDA_FOP(inodelk)
IDA_FOP(finodelk)
IDA_FOP(link)
IDA_FOP(lk)
IDA_FOP(lookup)
IDA_FOP(mkdir)
IDA_FOP(mknod)
IDA_FOP(open)
IDA_FOP(opendir)
IDA_FOP(rchecksum)
IDA_FOP(readdir)
IDA_FOP(readdirp)
IDA_FOP(readlink)
IDA_FOP(readv)
IDA_FOP(removexattr)
IDA_FOP(fremovexattr)
IDA_FOP(rename)
IDA_FOP(rmdir)
IDA_FOP(setattr)
IDA_FOP(fsetattr)
IDA_FOP(setxattr)
IDA_FOP(fsetxattr)
IDA_FOP(stat)
IDA_FOP(fstat)
IDA_FOP(statfs)
IDA_FOP(symlink)
IDA_FOP(truncate)
IDA_FOP(ftruncate)
IDA_FOP(unlink)
IDA_FOP(writev)
IDA_FOP(xattrop)
IDA_FOP(fxattrop)

uintptr_t ida_get_inode_bad(xlator_t * xl, inode_t * inode)
{
    uint64_t value;
    ida_heal_t * heal;
    uintptr_t bad = 0;

    LOCK(&inode->lock);

    if ((__inode_ctx_get(inode, xl, &value) == 0) && (value != 0))
    {
        heal = (ida_heal_t *)(uintptr_t)value;

        bad = ~heal->available;
    }

    UNLOCK(&inode->lock);

    return bad;
}

uintptr_t ida_get_bad(xlator_t * xl, loc_t * loc1, loc_t * loc2, fd_t * fd)
{
    uintptr_t bad = 0;

    if ((loc1 != NULL) && (loc1->inode != NULL))
    {
        bad |= ida_get_inode_bad(xl, loc1->inode);
    }
    else if (fd != NULL)
    {
        bad |= ida_get_inode_bad(xl, fd->inode);
    }
    if ((loc2 != NULL) && (loc2->inode != NULL))
    {
        bad |= ida_get_inode_bad(xl, loc2->inode);
    }

    return bad;
}

#define IDA_GF_FOP(_fop, _req, _dfc, _loc1, _loc2, _fd) \
    int32_t ida_gf_##_fop(call_frame_t * frame, xlator_t * xl, \
                          SYS_ARGS_DECL((SYS_GF_ARGS_##_fop))) \
    { \
        int32_t required; \
        ida_private_t * ida = xl->private; \
        if (IDA_FOP_REQUIRE_##_req == IDA_FOP_REQUIRE_ONE) \
        { \
            required = 1; \
        } \
        else if (IDA_FOP_REQUIRE_##_req == IDA_FOP_REQUIRE_MIN) \
        { \
            required = ida->fragments; \
        } \
        else \
        { \
            required = ida->nodes; \
        } \
        if ((_fd != NULL) && (_loc1 != NULL)) \
        { \
            SYS_CALL( \
                ida_fd_ctx_create, (_fd, xl, _loc1), \
                E(), \
                GOTO(failed) \
            ); \
        } \
        SYS_ASYNC(ida_##_fop, (frame, xl, &ida_handlers_##_fop, \
                               IDA_FOP_MODE_##_dfc, \
                               ida_get_bad(xl, _loc1, _loc2, _fd), \
                               ida->fragments, required, _loc1, _loc2, _fd, \
                               SYS_ARGS_NAMES((SYS_GF_ARGS_##_fop)))); \
        return 0; \
    failed: \
        ida_handlers_##_fop.completed(frame, EIO, NULL, NULL); \
        return 0; \
    }


// Some fops need to wait for *all* answers because further operations could
// fail in server xlator. For example, a create request could be answered as
// soon as the minimum number of subvolumes have answered, however a future
// writev operation can fail because the server translator may not find the
// inode (even though that the dfc will later sort the requests to avoid an
// incorrect execution order)
// TODO: Determine exactly which operations need to be fully completed before
//       proceeding
IDA_GF_FOP(access,       ONE, DIO, loc,    NULL,   NULL)
IDA_GF_FOP(create,       ALL, DFC, loc,    NULL,   fd)
IDA_GF_FOP(entrylk,      MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(fentrylk,     MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(flush,        MIN, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(fsync,        MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(fsyncdir,     MIN, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(getxattr,     ONE, DIO, loc,    NULL,   NULL)
IDA_GF_FOP(fgetxattr,    ONE, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(inodelk,      MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(finodelk,     MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(link,         MIN, DFC, oldloc, newloc, NULL)
IDA_GF_FOP(lk,           MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(lookup,       MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(mkdir,        ALL, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(mknod,        ALL, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(open,         ALL, DFC, loc,    NULL,   fd)
IDA_GF_FOP(opendir,      ALL, DFC, loc,    NULL,   fd)
IDA_GF_FOP(rchecksum,    MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(readdir,      ONE, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(readdirp,     ONE, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(readlink,     ONE, DIO, loc,    NULL,   NULL)
IDA_GF_FOP(readv,        MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(removexattr,  MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(fremovexattr, MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(rename,       ALL, DFC, oldloc, newloc, NULL)
IDA_GF_FOP(rmdir,        MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(setattr,      MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(fsetattr,     MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(setxattr,     MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(fsetxattr,    MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(stat,         ONE, DIO, loc,    NULL,   NULL)
IDA_GF_FOP(fstat,        ONE, DIO, NULL,   NULL,   fd)
IDA_GF_FOP(statfs,       ALL, DIO, loc,    NULL,   NULL)
IDA_GF_FOP(symlink,      MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(truncate,     MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(ftruncate,    MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(unlink,       MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(writev,       MIN, DFC, NULL,   NULL,   fd)
IDA_GF_FOP(xattrop,      MIN, DFC, loc,    NULL,   NULL)
IDA_GF_FOP(fxattrop,     MIN, DFC, NULL,   NULL,   fd)

int32_t ida_gf_forget(xlator_t * this, inode_t * inode)
{
    uint64_t value;
    ida_inode_ctx_t * ctx;

    if ((inode_ctx_del(inode, this, &value) == 0) && (value != 0))
    {
        ctx = (ida_inode_ctx_t *)value;
        SYS_FREE(ctx);
    }

    return 0;
}

int32_t ida_gf_invalidate(xlator_t * this, inode_t * inode)
{
    return 0;
}

int32_t ida_gf_release(xlator_t * this, fd_t * fd)
{
    uint64_t value;
    ida_fd_ctx_t * ctx;

    if ((fd_ctx_del(fd, this, &value) == 0) && (value != 0))
    {
        ctx = (ida_fd_ctx_t *)(uintptr_t)value;
        loc_wipe(&ctx->loc);
        SYS_FREE(ctx);
    }

    return 0;
}

int32_t ida_gf_releasedir(xlator_t * this, fd_t * fd)
{
    uint64_t value;
    ida_fd_ctx_t * ctx;

    if ((fd_ctx_del(fd, this, &value) == 0) && (value != 0))
    {
        ctx = (ida_fd_ctx_t *)(uintptr_t)value;
        loc_wipe(&ctx->loc);
        SYS_FREE(ctx);
    }

    return 0;
}

SYS_GF_FOP_TABLE(ida_gf);
SYS_GF_CBK_TABLE(ida_gf);

struct volume_options options[] =
{
    {
        .key = { "size" },
        .type = GF_OPTION_TYPE_STR,
        .description = "Size of the dispersion represented as X:Y. "
                       "Where X is the number of nodes and Y is the redundancy."
    },
    {
        .key = { "write-extra" },
        .type = GF_OPTION_TYPE_INT,
        .description = "Number of extra answers required to consider an "
                       "operation as valid before propagating the result to the "
                       "upper translators. 0 means that is only required the "
                       "minimum. A negative number means that the answer will "
                       "be propagated before the minimum number of required "
                       "childs have completed. This could improve performance "
                       "but it can be very dangerous and lead to data corruption."
    },
    {
        .key = { "block-size" },
        .type = GF_OPTION_TYPE_INT,
        .description = "File system block size"
    },
    { }
};
