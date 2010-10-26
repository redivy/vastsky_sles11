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

__version__ = '$Id: lvnode_3.py 319 2010-10-20 05:54:09Z yamamoto2 $'

# Dext module

import os
import time
import mynode
import lvnode
import hashedlock
import threading
from vas_conf import *
from vas_subr import dmsetup_remove, executecommand, executecommand_retry, \
    execute_retry_not_path_exist, execute_retry_path_exist, getDextDevName, \
    getDmDevPath, vbtos

def all(xs):
    return reduce(lambda x, y: x and y, xs, True)

def extract_pdskid_from_iscsi_path(iscsi_path):
    return iscsi_path.split(':')[-1].split('-lun-1')[0]

def extract_ip_from_iscsi_path(iscsi_path):
    return iscsi_path.split(':')[0].split('/dev/disk/by-path/ip-')[1]

iscsiadm_lock = threading.Lock() # workaround for bug ID: 3057168

def executeiscsiadm(command):
    iscsiadm_lock.acquire()
    try:
        ret = executecommand(command)
    except:
        iscsiadm_lock.release()
        raise
    iscsiadm_lock.release()
    return ret

def loginIscsiTarget(paths):
    def have_devices():
        return all(map(os.path.exists, paths))
    if have_devices():
        logger.debug("loginIscsiTarget: nothing to do")
        return
    pdskid_str = extract_pdskid_from_iscsi_path(paths[0])
    iqn = "iqn.%s:%s" % (iqn_prefix_iscsi, pdskid_str)
    def login(path):
        ip_str = extract_ip_from_iscsi_path(path)
        logger.debug("iscsi new: iqn %s portal %s path %s" % \
            (iqn, ip_str, path))
        assert pdskid_str == extract_pdskid_from_iscsi_path(path)
        executeiscsiadm("iscsiadm -m node -o new -T %s -p %s" % (iqn, ip_str))
    map(login, paths)
    logger.debug("iscsi login: iqn %s" % iqn)
    command = "iscsiadm -m node -T %s --login" % iqn
    def condition():
       return not have_devices()
    executecommand_retry(command, condition, ISCSIADM_RETRY_TIMES)
    time_left = LOGIN_TIMEOUT
    while not have_devices() and time_left > 0:
        time_left -= 1
        time.sleep(1)
    map(getMultiPathDevice, paths)

def getMultiPathDevice(dev_disk_by_path):
    # input: /dev/disk/by-path/ip-10.100.10.3:3260-iscsi-iqn.2000-11.jp.co.valinux:00000006-lun-1
    # output: /dev/mapper/mpath5
    dev_path = ''

    # iSCSI device path --> wwid
    wwid = ''

    command = "scsi_id -g -u -s /block/%s" % os.path.realpath(dev_disk_by_path).split('/dev/')[1]
    def condition():
        return os.path.exists(dev_disk_by_path)
    wwid = executecommand_retry(command, condition, SCSI_ID_RETRY_TIMES)

    # wwid --> Multipath device path
    dev_name = ''

    for i in range(0, GETMULTIPATHDEVICE_RETRY_TIMES):
        try:
            executecommand("multipath")
            dev_name = executecommand("dmsetup info --noheadings -c -u mpath-%s -o name" % (wwid))
            return getDmDevPath(dev_name)
        except:
            time.sleep(GETMULTIPATHDEVICE_RETRY_INTERVAL)

    logger.error("getMultiPathDevice: multipath -v3: %s", executecommand("multipath -v3"))
    raise Exception, "retry over"

def cleanupMultiPathDevice(path):
    pdskid_str = extract_pdskid_from_iscsi_path(path)
    dev_name = getMultiPathDevice(path).split(DM_DEVICE_DIR + '/')[1]
    # NOTE: the multipath command returns 1. (bug?)
    executecommand("/sbin/multipath -f %s" % dev_name, status_ignore=True)

    command = "iscsiadm -m node -T iqn.%s:%s --logout" % (iqn_prefix_iscsi, pdskid_str)
    execute_retry_path_exist(command, path, ISCSIADM_RETRY_TIMES )

    executeiscsiadm("iscsiadm -m node -o delete -T iqn.%s:%s" % (iqn_prefix_iscsi, pdskid_str) )

iscsi_lock = hashedlock.HashedLock()

# IscsiNode: iscsi+multipath
class IscsiNode(mynode.MyNode):
    def __init__(self, path):
        self.iscsi_path = path
        self.iscsi_ref = None
        mynode.MyNode.__init__(self, \
            "iscsi:%s" % extract_pdskid_from_iscsi_path(self.iscsi_path[0]))
    def do(self):
        h = iscsi_lock.acquire(self.iscsi_path)
        try:
            loginIscsiTarget(self.iscsi_path)
            self.path = getMultiPathDevice(self.iscsi_path[0])
            # keep a reference to prevent a detach operation from tearing down
            # our mpath+iscsi before we set up dext on it.
            self.iscsi_ref = os.open(self.path, os.O_RDONLY)
        except:
            iscsi_lock.release(h)
            raise
        iscsi_lock.release(h)
    def undo(self):
        def check_safe_logout(path):
            if not os.path.exists(path):
                return False
            devname = os.path.realpath(path).split('/dev/')[1]
            mpaths = os.listdir("/sys/block/%s/holders/" % devname)
            if len(mpaths) != 1:
                # holders exist other than dm_multipath
                return False
            holders_dir = "/sys/block/%s/holders/%s/holders" % \
                (devname, mpaths[0])
            if len(os.listdir(holders_dir)) != 0:
                # holders exist
                return False
            return True
        if self.iscsi_ref != None:
            os.close(self.iscsi_ref)
            self.iscsi_ref = None
        h = iscsi_lock.acquire(self.iscsi_path)
        try:
            if check_safe_logout(self.iscsi_path[0]):
                cleanupMultiPathDevice(self.iscsi_path[0])
                time_left = LOGIN_TIMEOUT
                paths = self.iscsi_path[:]
                while len(paths) and time_left > 0:
                    for path in paths:
                        i = paths.index(path)
                        if not os.path.exists(path):
                            paths.pop(i)
                        else:
                            time_left -= 1
                            time.sleep(1)
                if paths:
                    logger.error("IscsiNode logout timeout: %s", paths)
            self.path = None
        except:
            iscsi_lock.release(h)
            raise
        iscsi_lock.release(h)

class DextNode(lvnode.LVNode):
    def __init__(self, lvolstruct):
        self.dext_ssvrid = lvolstruct['lvolspec']['ssvrid']
        self.dext_offset = lvolstruct['lvolspec']['offset']
        lvnode.LVNode.__init__(self, lvolstruct, "dext")
    def do(self):
        assert(len(self.components) == 1)
        c = self.components[0]
        assert c.iscsi_ref != None
        name = getDextDevName(self.dext_ssvrid ,self.lvolid)
        path = getDmDevPath(name)
        if not os.path.exists(path):
            command = "echo 0 %s linear %s %s | dmsetup create %s" % \
                (vbtos(self.capacity), c.path, vbtos(self.dext_offset), name)
            execute_retry_not_path_exist(command, path, DMSETUP_RETRY_TIMES)
        self.path = path
        os.close(c.iscsi_ref)
        c.iscsi_ref = None
    def undo(self):
        assert(len(self.components) == 1)
        name = getDextDevName(self.dext_ssvrid ,self.lvolid)
        path = getDmDevPath(name)
        if os.path.exists(path):
            dmsetup_remove(name)
        self.path = None

def create(l, regnode, add_component, components):
    n = DextNode(l)
    iscsi = IscsiNode(l['lvolspec']['iscsi_path'])
    regnode(iscsi)
    add_component(n, iscsi)
    return n
