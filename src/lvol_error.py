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

__version__ = '$Id: lvol_error.py 319 2010-10-20 05:54:09Z yamamoto2 $'

import sys
import os
import re
import time
import signal
from vas_const import TARGET
from vas_conf import *
from vas_subr import *

mirrordev_re = re.compile("^[0-f]+$")
dext_re = re.compile("^[0-f]+\-[0-f]+$")
mddev_re = re.compile("^dev-")

def mirrorid_to_md(mirrorid):   # returns md dev name (ex. md255) of the mirror
    md = None
    try:
        stbuf = os.stat(getMirrorDevPath(mirrorid))
        md = 'md%d' % (os.minor(stbuf.st_rdev))
    except:
        logger.debug('mirrorid_to_md fail %d' % (mirrorid))
    return md

def get_mirror_status(mirrorid):
    status = {}
    md = mirrorid_to_md(mirrorid)
    if md:
        for dm in get_dmdevs(md):
            state = get_dm_state(md, dm)
            dextid = dm_to_dextid(md, dm)
            status[dextid] = state
    return status

def find_mirrors():     # returns a list of mirrorid in the system
    mirrors = []
    for dent in os.listdir(MD_DEVICE_DIR):
        if mirrordev_re.match(dent):
            mirrors.append(int(dent, 16))
        else:
            logger.debug('unknown entry "%s" found in %s' % (dent, MD_DEVICE_DIR))
    return mirrors

def get_dmdevs(md):
    md_path = '/sys/block/%s/md' % (md)
    dmdevs = []
    for dent in os.listdir(md_path):
        if mddev_re.match(dent):        # ex. dev-dm-20
            dmdevs.append(dent[4:])     # ex. dm-20
    return dmdevs

def get_dm_state(md, dm):
    f = open('/sys/block/%s/md/dev-%s/state' % (md, dm))
    line = f.readline()
    f.close()
    return line.rstrip()

def get_dm_devno(md, dm):
    f = open('/sys/block/%s/md/dev-%s/block/dev' % (md, dm))
    line = f.readline()
    f.close()
    (major, minor) = re.split(' ', line.replace(':', ' '))
    return int(major), int(minor)

def devno_to_dextid(major, minor):
    for dent in os.listdir(DM_DEVICE_DIR):
        if dext_re.match(dent):
            stbuf = os.stat(getDmDevPath(dent))
            if major == os.major(stbuf.st_rdev) and minor == os.minor(stbuf.st_rdev):
                id = int(dent[9:], 16)
                return id
    return 0

def dm_to_dextid(md, dm):
    major, minor = get_dm_devno(md, dm)
    id = devno_to_dextid(major, minor)
    if id == 0:
        logger.debug('no extent found (%s %s)' % (md, dm))
    return id

def main():
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    while True:
        time.sleep(LVOL_ERROR_INTERVAL)

        mirrors = find_mirrors()
#        logger.debug('mirrors %s' % (mirrors))

        faulty_list = []
        for mid in mirrors:
#            logger.debug('%s' % (get_mirror_status(mid)))
            md = mirrorid_to_md(mid)
            if not md:
                continue
            for dm in get_dmdevs(md):
                state = get_dm_state(md, dm)
                if state == 'faulty':
                    dextid = dm_to_dextid(md, dm)
                    if dextid != 0:
                        faulty_list.append(dextid)

        for lvol in faulty_list:
            pid = os.fork()
            if pid != 0:
                continue
            try:
                subargs = {'ver': XMLRPC_VERSION, 'target': TARGET['LVOL'], 'targetid': lvol}
                send_request(host_storage_manager_list, port_storage_manager, "notifyFailure", subargs)
            except:
                logger.error("notifyFailure LVOL %08x was called, but got an error." % (lvol))
            else:
                logger.info("notifyFailure LVOL %08x was called" % (lvol))
            os._exit(0)

if __name__ == '__main__':
    main()
