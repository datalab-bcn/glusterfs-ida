Disperse translator for GlusterFS
=================================

Introduction
------------

The **disperse** translator is a new kind of clustered alternative for
[GlusterFS](http://gluster.org) based on a mathematical construct called
[erasure codes](http://en.wikipedia.org/wiki/Erasure_code) which allows you to
configure the desired level of redundancy (how many bricks can fail without
service interruption) with a minimum cost in terms of storage space.

An erasure code is a mathematical way of transforming a chunk of data into a
set of data blocks of smaller size that can be recombined to recover the
original information. The key point is that not all data blocks are needed to
be able to reconstruct the original chunk of data, thus allowing some of them
to be missing. On a GlusterFS setup this means that each data block is stored
on a brick and, if a brick fails, the data blocks from the remaining bricks
are used to transparently recover the data.

There are many erasure codes. This translator is based on Rabin's IDA
(Information Dispersal Algorithm). It's quite easy and fast enough to handle
large amounts of information with a minimal performance impact.


Important notes
---------------

**DO NOT USE IT IN PRODUCTION ENVIRONMENTS**

This translator is experimental and currently it is **only intended for
developers**. The steps needed to get this translator running are quite complex
for an average user without programming knowledge. And if/when things go wrong
it may be difficult to understand what happened and how to solve the problem.

Once it matures, the installation and configuration will be much easier and it
will be ready for being used in production environments like any other
translator.


Requirements
------------

* Source code of GlusterFS 3.4.0 or later, configured, compiled and installed
* This version requires Intel SSE2 extensions to speed up the encoding (this
  requirement will be removed in the future)
* The [glusterfs-gfsys](https://forge.gluster.org/disperse/gfsys) library
* The [glusterfs-dfc](https://forge.gluster.org/disperse/dfc) translator is
  needed on all bricks
* The [glusterfs-heal](https://forge.gluster.org/disperse/heal) translator is
  needed on all bricks


Installation
------------

* Configure, compile and install GlusterFS and glusterfs-gfsys
* ./autogen.sh
* ./configure --with-glusterfs=<path to glusterfs> --with-gfsys=<path to gfsys>
* make
* make install

This should leave the translator modules into the same place where GlusterFS
has been installed.


Configuration
-------------

The easiest way to create a dispersed volume is to create a replicated volume
with the desired number of bricks and then manually modify the fuse volume
files before starting it, changing the type *cluster/replicate* by
*cluster/disperse* and adding the option *size*.

The option *size* has the format <num of bricks>:<redundancy>, where redundancy
is the maximum allowed number of bricks that can fail without losing of service.
It must be at least 1 and less than the half of the number of bricks.

Example to create a dispersed volume of 3 bricks with one of redundancy:

    gluster volume create ida replica 3 node1:/brick node2:/brick node3:/brick

The generated vol file should contain something like this:

    volume ida-replicate-0
        type cluster/replicate
        subvolumes ida-client-0 ida-client-1 ida-client-2
    end-volume

It should be modified this way:

    volume ida-replicate-0
        type cluster/disperse
        option size 3:1
        subvolumes ida-client-0 ida-client-1 ida-client-2
    end-volume

The volume name can be also changed if desired.


Technical information
---------------------

The implementation of IDA used for the disperse translator is based on the
multiplication of matrices composed by 8-bit numbers. All operations are
performed on a Galois Field of 8 bits. The multiplications are optimized using
Intel SSE2 xors. On average each 8-bit multiplication requires 31.3 xors,
however the most common numbers used can be computed in less than 20 xors.
Using 8 SSE2 registers it's possible to compute 128 parallel multiplications.
Additionally, current Intel processors can complete up to 3 SSE2 xors per cycle
(if there are no dependencies), giving really fast computations. On average, it
can compute 128 8-bit multiplications in about 10-15 CPU cycles. This means
that matrix multiplication is not a bottle neck because other things are much
slower.

However the reconstruction of the data requires to compute the inverse of a
matrix. This cost is high and can limit the read throughput. Matrix inversion
is not currently optimized. Better performance could be achived by using Cauchy
Matrices and a better implementation. Also some caching of recently inverted
matrices might also improve the performance a lot. However currently this does
not seem to be a problem and remains as something to do in the future.

If N is the number of required bricks to reconstruct the original data (N =
number of bricks - redundancy), then each file is split in chunks of 128\*N
bytes. Each chunk is then transformed into a block of 128\*(num of bricks)
bytes, and a slice of 128 bytes from this block is sent to each brick. To
recover the information, at least N bricks must be read (128 bytes from each
brick) to rebuild the original data chunk.

On writes not aligned to 128\*N bytes an additional read of 128\*N bytes is
needed before writing (Read-Modify-Write cycle). Also, if the end of the buffer
to write does not fully fill a block of 128\*N bytes, a second read of 128\*N
bytes is needed. This is necessary to preserve the unmodified fragment of the
first and last written blocks.

Each brick stores a file smaller than the original by a factor of N. Having B
bricks of capacity C, with a redundancy of R (at most R bricks may fail without
service interruption), the total available size of the volume will be C\*N, and
the proportion af wasted space will be R/B.

For example:

    A volume composed by 8 bricks of 4 TB with a redundancy of 3 will have:

        Total raw capacity: 32 TB
        Effectice capacity: 20 TB
        Wasted space:       12 TB (37.5%)

A distributed-replicated volume with the same effective capacity would require
10 bricks of 4 TB instead of 8, and only one brick could fail safely (if you
are unlucky and the two bricks of a replica fail, you will lose data). With the
disperse translator any 3 of the 8 bricks can fail simulatenously without any
problem.

To achieve a similar guaranteed redundancy for a distributed-replicated volume
of 20 TB, 20 bricks of 4 TB would be necessary.

    Characteristics of similar distributed-replicated volumes:

      Same capacity, minimal bricks:        Same capacity, same redundancy:

        Number of bricks:   10                Number of bricks:   20
        Total raw capacity: 40 TB             Total raw capacity: 80 TB
        Effective capacity: 20 TB             Effective capacity: 20 TB
        Wasted space:       20 TB (50.0%)     Wasted space:       60 TB (75.0%)

The real size of the file is stored as an extended attribute. An additional
extended attribute is used for versioning to detect inconsistencies.


Known problems
--------------

On files being simultaneously read and written by one or more clients without
any external synchronization between them may result in read failures. This is
caused by the possibility that the read operation be performed in different
order with respect to the writes on each brick. The read needs that at least N
fragments from N bricks match the version to return a valid result. It's not a
problem if the read returns a mismatching version on some of the bricks (less
than or equal to R). This is considered a very rare case now and it will be
addressed by a more global solution in the near future.

Performance is quite bad right now. There are many optimizations to incorporate
to the translator (specially on write operations) to achieve production level
requirements.

Code quality will need to be improved (some structural changes, code cleaning
and adding documentation).

