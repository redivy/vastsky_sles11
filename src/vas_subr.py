

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

__version__ = '$Id: vas_subr.py 108 2010-07-22 09:22:17Z yamamoto2 $'

import time
import socket
import xmlrpclib
import errno
import os
import inspect
import commands
import re
import pickle
from vas_conf import *
from stat import *

def get_targetid(input, prefix):
    try:
        if input.index(prefix) == 0:
            prefixlen = len(prefix)
            hexid = input[prefixlen:]
            if len(hexid) == 8:
                return int(hexid, 16)

    except ValueError:
        try:
            if len(input) == 8:
                return int(input,16)
        except ValueError:
            pass

    raise ValueError, "invalid %sid: %s" % (prefix, input)

def send_request(ip_addrs, port, method, data):
    res = None
    for ip in ip_addrs:
        try:
            agent = xmlrpclib.ServerProxy("http://%s:%s" % (ip, port))
            func = getattr(agent, method)
            res = func(data)
            return res
        except socket.timeout, inst:
            # timeout
            logger.error("send_request: timeout: %s", (inst))
            raise xmlrpclib.Fault(errno.ETIMEDOUT, 'ETIMEDOUT')
        except xmlrpclib.Fault, inst:
            # Exceptions on remote side
            raise
        except Exception, inst:
            # try other link
            continue
    # both link down or server down
    raise xmlrpclib.Fault(errno.EHOSTDOWN, 'EHOSTDOWN')

def daemonize(stdin = '/dev/null', stdout = '/dev/null', stderr = '/dev/null'):
    """ daemonize() """
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError:
        sys.exit(1)
    os.chdir("/")
    os.setsid()
    os.umask(0)
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError:
        sys.exit(1)
    si = file(stdin, 'r')
    so = file(stdout, 'a')
    se = file(stderr, 'a', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    si.close()
    so.close()
    se.close()

    return os.getpid()

def lineno():
    return inspect.currentframe().f_back.f_lineno

# 1GB = 1024MB = 2097152 sectors
def gtos(giga):
    return giga * 1024 * 1024 * 1024/ 512

def stog(sector):
    giga = sector * 512 / 1024 / 1024 / 1024
    odd = sector - gtos(giga)
    return [giga, odd]

# 1vb = (1024+1)MB = 2099200 sectors
def vbtos(vb):
    return vb * 1024 * 1024 * (1024 + 1)/ 512

def stovb(sector):
    vb = sector * 512 / 1024 / 1024 / (1024 + 1)
    odd = sector - vbtos(vb)
    return [vb, odd]

def executecommand(command):
    status, output = commands.getstatusoutput(command)
    if status != 0:
        errstr = "%s [Status %s] %s" % (command,status,output)
        raise Exception, errstr

    logger.debug(command)

    return output

def executecommand_retry(command,condition,count):
    assert(count), lineno()

    for i in range(0,count):
        try:
            output = executecommand(command)
            if i:
                logger.info("retry OK")
            return output
        except Exception, inst:

            # for condition use
            command_output = str(inst)
            logger.info(command_output)

            if not eval(condition):
                logger.info("quit retry(%d/%d)" % (i + 1, count))
                return ""
            logger.info("retrying(%d/%d) ..." % (i + 1, count))
            time.sleep(i + 1)
            

    errstr = "retry over"
    raise Exception, errstr

def execute_retry_not_path_exist(command,path,count):
    for i in range(0,count):
        if os.path.exists(path):
            return
        try:
            executecommand(command)
            if i:
                logger.info("retry OK")
            return
        except Exception, inst:
            command_output = str(inst)
            logger.info(command_output)
            logger.info("retrying(%d/%d) ..." % (i + 1, count))
            time.sleep(i + 1)
    raise Exception, "retry over"

def execute_retry_path_exist(command,path,count):
    for i in range(0,count):
        if not os.path.exists(path):
            return
        try:
            executecommand(command)
            if i:
                logger.info("retry OK")
            return
        except Exception, inst:
            command_output = str(inst)
            logger.info(command_output)
            logger.info("retrying(%d/%d) ..." % (i + 1, count))
            time.sleep(i + 1)
    raise Exception, "retry over"

def dispatch_and_log(obj, method, params):
    logger.info("DISPATCH %s called. %s" % (method, params))
    if method == "helloWorld":
        return 0
    try:
        f = getattr(obj, method)
    except AttributeError:
        raise Exception('method "%s" is not supported' % method)
    except Exception, inst:
        logger.info("DISPATCH getattr(%s) EXCEPTION %s" % \
        (method, inst))
        raise
    try:
        if not params[0].has_key('ver') or params[0]['ver'] != XMLRPC_VERSION:
            raise xmlrpclib.Fault(errno.EPROTO, 'EPROTO')
        ret = f(*params)
    except Exception, inst:
        logger.info("DISPATCH %s EXCEPTION %s" % (method, inst))
        raise
    logger.info("DISPATCH %s returned %s" % (method, ret))
    return ret

def get_lvolstruct_of_lvoltype(lvolstruct, type):
    if lvolstruct['lvoltype'] != type:
        if not lvolstruct['components']:
            return None
        else:
            return get_lvolstruct_of_lvoltype(lvolstruct['components'][0], type)
    else:
        return lvolstruct

def addNewTargetIET(tid, iqn, pdskid):
    executecommand("ietadm --op new --tid=%d --params Name=%s:%08x" % (tid, iqn, pdskid))
    executecommand("ietadm --op update --tid=%d --params=%s" % (tid, IETADM_PARAMS_TID))

def delNewTargetIET(tid):
    executecommand("ietadm --op delete --tid=%d" % (tid))

def activatePhysicalDisk(data):

    def save_data_file():
        # save request first.
        datafile = "%s/pdsk-%08x" % (STORAGE_MANAGER_VAR, pdskid)
        f = open(datafile, "w")
        pickle.dump(data, f)
        f.close()

    disk_dev = data['disk_dev']
    pdskid = data['pdskid']
    iqn = data['iqn']

    if SCSI_DRIVER == 'srp':
        activatePhysicalDiskSRP(disk_dev, pdskid)
    elif SCSI_DRIVER == 'iet':
        activatePhysicalDiskIET(disk_dev, iqn, pdskid)
    save_data_file()


def scan_tid(tid):
    tid_re = re.compile("^tid:\d+$")

    f = open("/proc/net/iet/volume", 'r')
    while True:
        line = f.readline().rstrip()
        if not line:
            break
        tid_colon_num = line.split()[0]
        if tid_re.match(tid_colon_num):
            if int(tid_colon_num.split(':')[1], 10) == tid:
                return True
    return False

def get_tid(pdskid):
    tid_re = re.compile("^tid:\d+$")

    f = open("/proc/net/iet/volume", 'r')
    while True:
        line = f.readline().rstrip()
        if not line:
            break
        tid_colon_num = line.split()[0]
        if tid_re.match(tid_colon_num):
            if int(line.split(':')[-1], 16) == pdskid:
                return int(line.split()[0].split(':')[1], 10)

    raise Exception, "get_tid: can not identify tid correspond to pdsk-%08x" % (pdskid)

class getDiskSizeNotBlockDeviceException:
    pass

def getDiskSize(devpath):

    mode = os.stat(devpath)[ST_MODE]
    if not S_ISBLK(mode):
        raise getDiskSizeNotBlockDeviceException

    output = executecommand("blockdev --getsize %s" % devpath)
    return int(output, 10)

def gen_tid():
    tid_re = re.compile("^tid:\d+$")

    tids = []
    f = open("/proc/net/iet/volume", 'r')
    while True:
        line = f.readline().rstrip()
        if not line:
            break
        tid_colon_num = line.split()[0]
        if tid_re.match(tid_colon_num):
            tid = int(tid_colon_num.split(':')[1], 10)
            tids += [tid]
    for i in range(MIN_TID, MAX_TID):
        if i not in tids:
            return i

    raise Exception, "gen_tid: can not get a new tid"

def createPdskLink(disk_dev, pdskid):
    if not os.path.exists(VAS_PDSK_DIR):
        os.mkdir(VAS_PDSK_DIR)
    removePdskLink(pdskid)
    path = "%s/%08x" % (VAS_PDSK_DIR, pdskid)
    logger.debug("createPdskLink: %s -> %s" % (path, disk_dev))
    executecommand("ln -s %s %s" % (disk_dev, path))
    return path

def removePdskLink(pdskid):
    path = "%s/%08x" % (VAS_PDSK_DIR, pdskid)
    if not os.path.exists(path):
        return
    logger.debug("removePdskLink: %s" % path)
    executecommand("rm %s" % (path))

def addNewLogicalUnitIET(pdskid, tid, path):
    executecommand("ietadm --op new --tid=%d --lun=1 --params Path=%s,ScsiId=%08x,ScsiSN=%08x,%s" % (tid, path, pdskid, pdskid, IETADM_PARAMS_LUN))

class getDeviceListBadFileException:
    pass

def getDeviceList(path):

    pdsk_re = re.compile("^pdsk-[0-f]+$")
    pdsk_devpath_list = []
    pdskid_list = []

    if path not in (REGISTER_DEVICE_LIST, DEVICE_LIST_FILE):
        logger.error("device path %r is not in %r, %r" % (path, REGISTER_DEVICE_LIST, DEVICE_LIST_FILE))
        raise getDeviceListBadFileException

    if not os.path.exists(path):
        logger.error("device path %r does not exist" % (path))
        raise getDeviceListBadFileException
    
    f =  open(path, 'r')
    ln = 0
    while True:
        line = f.readline().strip()
        ln += 1
        if not line:
            break
        try:
            title, devpath = line.split(',')
        except ValueError:
            continue
        try:
            real_devpath = os.path.realpath(devpath)
            getDiskSize(real_devpath)
        except:
            logger.error("%s(%d): %s: can not determine device capacity." % (path, ln, devpath))
            raise getDeviceListBadFileException

        if path == REGISTER_DEVICE_LIST:
            if title == 'pdsk':
                pdsk_devpath_list.append(devpath)
            else:
                continue
        else:
            # DEVICE_LIST_FILE
            if pdsk_re.match(title):
                pdsk_devpath_list.append(devpath)
                pdskid_list.append(get_targetid(title, 'pdsk-'))
            else:
                continue

    if path == REGISTER_DEVICE_LIST:
        return pdsk_devpath_list
    else:
	return (pdsk_devpath_list, pdskid_list)

def getMultiPathDevice(dev_disk_by_path):
    # input: /dev/disk/by-path/ip-10.100.10.3:3260-iscsi-iqn.2009-06.jp.co.valinux:00000006-lun-1
    # output: /dev/mapper/mpath5
    dev_path = ''

    # iSCSI device path --> wwid
    wwid = ''

    command = "scsi_id -g -u /dev/%s" % os.path.realpath(dev_disk_by_path).split('/dev/')[1]
    condition = "os.path.exists('"'%s'"')" % dev_disk_by_path
    wwid = executecommand_retry(command, condition, SCSI_ID_RETRY_TIMES)

    # wwid --> Multipath device path
    dev_name = ''

    command = "dmsetup info --noheadings -c -u mpath-%s -o name" % (wwid)
    condition = "os.path.exists('"'%s'"')" % dev_disk_by_path
    dev_name = executecommand_retry(command, condition, DMSETUP_RETRY_TIMES)

    dev_path = getDmDevPath(dev_name)

    return dev_path

def cleanupMultiPathDevice(path):
    pdskid_str = path.split(':')[-1].split('-lun-1')[0]
    dev_name = getMultiPathDevice(path).split(DM_DEVICE_DIR + '/')[1]
    # NOTE: the multipath command returns 1. (bug?)
    commands.getstatusoutput("/sbin/multipath -f %s" % dev_name)
    iscsiLogout(path, pdskid_str, iqn_prefix_iscsi)

def setupDextDevice(lvolstruct_extent):
    devname = getDextDevName(lvolstruct_extent['lvolspec']['ssvrid'],lvolstruct_extent['lvolid'])
    devpath = getDmDevPath(devname)

    dev_path = getMultiPathDevice(lvolstruct_extent['lvolspec']['iscsi_path'][0])
    command = "echo 0 %s linear %s %s | dmsetup create %s" % (vbtos(lvolstruct_extent['capacity']),dev_path,vbtos(lvolstruct_extent['lvolspec']['offset']),devname)
    execute_retry_not_path_exist(command, devpath, DMSETUP_RETRY_TIMES)

    return devpath

def getRoundUpCapacity(capacity):
    unit = min(EXTENTSIZE)
    return ( ( capacity + ( unit - 1 ) ) / unit * unit )

def get_arg_max():
    arg_max_str = executecommand(GETCONF_ARG_MAX)
    return int(arg_max_str, 10)

def getDextDevName(ssvrid, dextid):
    return "%08x-%08x" % (ssvrid, dextid)

def getMirrorDevName(lvolid):
    return "%d" % (lvolid)

def getLinearDevName(lvolid):
    return "lvol-%08x" % (lvolid)

def getMetaDevName(lvolid):
    return "meta-%08x" % (lvolid)

def getDataDevName(lvolid):
    return "data-%08x" % (lvolid)

def getDmDevPath(devname):
    return "%s/%s" % (DM_DEVICE_DIR, devname)

def getDextDevPath(ssvrid, dextid):
    return getDmDevPath(getDextDevName(ssvrid, dextid))

def getMirrorDevPath(lvolid):
    return "%s%s" % (MD_DEVICE_DIR, getMirrorDevName(lvolid))

def getLinearDevPath(lvolid):
    return getDmDevPath(getLinearDevName(lvolid))

def getMetaDevPath(lvolid):
    return getDmDevPath(getMetaDevName(lvolid))

def getDataDevPath(lvolid):
    return getDmDevPath(getDataDevName(lvolid))

def check_lvolname(lvolname):
    if len(lvolname) == 0:
        raise Exception, "invalid lvolname('')."
    if len(lvolname) > 64:
        raise Exception, "lvolname too long."
    if lvolname == "." or lvolname == "..":
        raise Exception, "invalid lvolname(%s)." % lvolname
    for c in lvolname:
        if c not in VALID_CHARACTERS_FOR_LVOLNAME:
            raise Exception, "invalid lvolname(%s)." % lvolname
    lvol_re = re.compile("^lvol-[0-f]+$")
    if lvol_re.match(lvolname):
        raise Exception, "invalid lvolname(%s)." % lvolname

# IET and iscsi-initiator functions
#

def activatePhysicalDiskIET(disk_dev, iqn, pdskid):
    try:
        tid = get_tid(pdskid)
        # already registered.
        logger.info("activatePhysicalDiskIET: %s:%08x is already activated. tid = %d" % (iqn, pdskid, tid))
        return
    except:
        pass
        # fail through

    tid = gen_tid()
    try:
	path = createPdskLink(disk_dev, pdskid)
        # setup iSCSI target
        addNewTargetIET(tid, iqn, pdskid)
        # setup iSCSI LUN and save data file
        addNewLogicalUnitIET(pdskid, tid, path)
    except:
        cleanupPhysicalDiskIET(pdskid, tid)
        raise

def cleanupPhysicalDiskIET(pdskid, tid):
    if scan_tid(tid):
        delNewTargetIET(tid)
    removePdskLink(pdskid)

def iscsiLogout(path, pdskid_str, iqn_prefix_iscsi):
    command = "iscsiadm -m node -T iqn.%s:%s --logout" % (iqn_prefix_iscsi, pdskid_str)
    execute_retry_path_exist(command, path, ISCSIADM_RETRY_TIMES )

    executecommand("iscsiadm -m node -o delete -T iqn.%s:%s" % (iqn_prefix_iscsi, pdskid_str) )


def activatePhysicalDiskSRP(disk_dev, pdskid):
    logger.info('activatePhysicalDiskSRP: creating symlink')
    try:
	path = createPdskLink(disk_dev, pdskid)
    except:
        removePdskLink(pdskid)
        raise

