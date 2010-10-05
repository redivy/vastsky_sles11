#!/usr/bin/python

# Copyright (c) 2010 VA Linux Systems Japan K.K. All rights reserved.
#
# LICENSE NOTICE
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the Company nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COMPANY AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COMPANY OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

__version__ = '$Id: mdstat.py 24 2010-07-05 02:58:29Z yamamoto2 $'

import commands
import re
import sys
import os
from vas_db import MIRROR_STATUS

def __getMirrorDevList():
    "get mirror device list. {<minor>:, '<path>', ...}"
    # input: none
    # output: {0: '/dev/md0', ... , 255: '/dev/md255'}

    mddev_re = re.compile("^md\d+$")
    mds = {}
    for dent in os.listdir('/dev/'):
        if mddev_re.match(dent):
            path = '/dev/%s' % (dent)
            stbuf = os.stat(path)
            mds[os.minor(stbuf.st_rdev)] = path
    return mds

def __getMirrorIdList():
    "get mirror device lvolid list. {<minor>:, '/dev/mirror/<lvolid>', ...}"
    # input: none
    # output: {249: '/dev/mirror/00000091', ... , 255: '/dev/mirror/00000059'}

    mirrordev_re = re.compile("^[0-f]+$")
    mirror_ids = {}
    for dent in os.listdir('/dev/mirror/'):
        if mirrordev_re.match(dent):
            path = '/dev/mirror/%s' % (dent)
            stbuf = os.stat(path)
            mirror_ids[os.minor(stbuf.st_rdev)] = path
    return mirror_ids

def __getDextIdList():
    "get disk extent lvolid list. {<minor>:, '<lvolid>', ...}"
    # input: none
    # output: {41: '00000087', ... , 28: '0000004a'}

    dext_re = re.compile("^[0-f]+\-[0-f]+$")
    dext_ids = {}
    for dent in os.listdir('/dev/mapper/'):
        if dext_re.match(dent):
            path = '/dev/mapper/%s' % (dent)
            stbuf = os.stat(path)
            dext_ids[os.minor(stbuf.st_rdev)] = dent[9:]
    return dext_ids

def getMirrorList(lvol, mds_cache, mirror_ids_cache):
    "get list of mirror devices constitute specified linear device. ({<minor>: '<device>', ...}, {<minor>: 'lvolid', ...})"
    # input: lvol-0000008d
    # output: ({253: 'md253', 254: 'md254', 255: 'md255'},{253: '00000061', 254: '0000005d', 255: '00000059'})

    deps = {}
    dep_ids = {}
    mds = {}
    mirror_ids = {}
    dmsetup_deps = 'LANG=C /sbin/dmsetup deps /dev/mapper/'
    cmd = dmsetup_deps + lvol

    if len(mds_cache) == 0:
        mds = __getMirrorDevList()
    else:
        mds = mds_cache

    if len(mirror_ids_cache) == 0:
        mirror_ids = __getMirrorIdList()
    else:
        mirror_ids = mirror_ids_cache

    # 3 dependencies  : (9, 253) (9, 254) (9, 255)
    status, output = commands.getstatusoutput(cmd)
    if status:
        raise
    # ['3 dependencies\t: ', '9, 253 ', '9, 254 ', '9, 255']
    mirrors = re.split('\(', output.replace(')', ''))
    nmirrors = len(mirrors)
    for m in range(1, nmirrors):
        (major, minor) = re.split(',', mirrors[m])
        # {253: 'md253', 254: 'md254', 255: 'md255'}
        deps[int(minor)] = mds[int(minor)].replace('/dev/', '')
        # {253: '0000003a', 254: '00000036', 255: '00000032'}
        dep_ids[int(minor)] = mirror_ids[int(minor)].replace('/dev/mirror/', '')
    return (deps, dep_ids, mds, mirror_ids)

def __mirrorIdToMdx(lvolid):
    "convert lvolid value to mirror device name(mdXXX)"

    # input: lvolid (not strings)
    # output: md255

    stbuf = os.stat('/dev/mirror/%08x' % (lvolid))
    return 'md%d' % os.minor(stbuf.st_rdev)

def getDextList(mdx, dext_ids_cache):
    "get sync status of disk extents constitute specified mirror device. ( {<index>: 'lvolid', ...},{<index>: 'status', ...})"
    # input: md255 or 00000032
    # output: ({135: 'in_sync', ... , 149: 'in_sync'},{41: '00000087', ... , 28: '0000004a'})

    dext_ids = {}

    try:
        lvolid = int(mdx,16)
        mdx = __mirrorIdToMdx(lvolid)
    except:
        pass

    if len(dext_ids_cache) == 0:
        dext_ids = __getDextIdList()
    else:
        dext_ids = dext_ids_cache

    devdm_re = re.compile("^dev-dm-\d+$")
    minors = []
    for dent in os.listdir('/sys/block/%s/md/' % (mdx)):
        if devdm_re.match(dent):
            minors.append(dent.replace('dev-dm-', ''))
    ndexts = len(minors)

    array = []
    for d in range(0, ndexts):
        f = open('/sys/block/%s/md/dev-dm-%s/block/dev' % (mdx,minors[d]))
        line = f.readline()
        (major, minor) = re.split(' ', line.replace(':', ' '))
        entry = {}
        entry['dextid'] = int(dext_ids[int(minor)],16)

        f = open('/sys/block/%s/md/dev-dm-%s/state' % (mdx,minors[d]))
        line = f.readline()
        entry['dext_status'] = line.replace('\n', '')
        array.append(entry)

    return (array, dext_ids)

def get_rebuild_status(lvolid_mirror):

    dexts, _ = getDextList("%08x" % lvolid_mirror, {})
    array = []
    for dext in dexts:
        entry = {}
        entry['dextid'] = dext['dextid']
        if dext['dext_status'] == 'in_sync':
            entry['dext_status'] = MIRROR_STATUS['VALID']
        else:
            entry['dext_status'] = MIRROR_STATUS['INVALID']
        array.append(entry)

    return array
