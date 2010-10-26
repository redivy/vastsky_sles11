

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

__version__ = '$Id: vas_subr.py 321 2010-10-20 05:59:06Z yamamoto2 $'

import time
import socket
import xmlrpclib
import errno
import os
import inspect
import re
import pickle
import select
import threading
from vas_conf import *
from stat import *
from vas_iscsi import select_iScsiTarget

iScsiTarget = select_iScsiTarget(ISCSI_TARGET)

def __reverse_dict(a):
    return dict(zip(a.values(), a.keys()))

def start_worker(func, *args):
    th = threading.Thread(target = func, args = args)
    th.setDaemon(True)
    th.start()

# check keys and return values
def mand_keys(dict, *keys):
    if len(keys) == 0:
        raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
    for key in keys: 
        if not dict.has_key(key):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
    if len(keys) == 1: # return a simple value
        return dict[keys[0]]
    return map((lambda x: dict[x]), keys)

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

# motivation of this function: subprocess::Popen is not thread safe
# much simpler and fast than Popen 
def executecommand(command, status_ignore=False, close_fds=True):
    so_r, so_w = os.pipe()
    se_r, se_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(so_r)
        os.close(se_r)
        os.dup2(so_w, 1)
        os.dup2(se_w, 2)
        # this is heavy. it is need for "mdadm --monitor" only now
        # if "mdadm --monitor" is removed, this(close_fds) will be removed too
        if close_fds == True:
            try:
                num_fds = os.sysconf("SC_OPEN_MAX")
            except:
                num_fds = 64
            for i in range(3, num_fds):
                try:
                    os.close(i)
                except:
                    pass
        args = ["/bin/sh", "-c", command]
        os.execvp(args[0], args)
    # parent
    os.close(so_w)
    os.close(se_w)
    so_str = ""
    se_str = ""
    rset = [so_r, se_r]
    while rset:
        rlist, _, _ = select.select(rset, [], [])
        if so_r in rlist:
            data = os.read(so_r, 1024)
            if data == "":
                os.close(so_r)
                rset.remove(so_r)
            so_str += data
        if se_r in rlist:
            data = os.read(se_r, 1024)
            if data == "":
                os.close(se_r)
                rset.remove(se_r)
            se_str += data
    _, status = os.waitpid(pid, 0)
    if status != 0 and status_ignore == False:
        errstr = "%s [Status %d] %s" % (command, status, se_str.rstrip())
        raise Exception, errstr
    logger.debug(command)
    return so_str.rstrip() 

# executecommand_retry:
# execute a command.  if failed, retry upto count times as far as
# the given condition is true.
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

            if not condition():
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

    try:
        tid = get_tid(pdskid)
        # already registered.
        logger.info("activatePhysicalDisk: %s:%08x is already activated. tid = %d" % (iqn, pdskid, tid))
        return
    except:
        pass
        # fail through

    tid = gen_tid()
    try:
        path = createPdskLink(disk_dev, pdskid)
        # setup iSCSI target
        iScsiTarget.newTarget(tid, iqn, pdskid)
        # setup iSCSI LUN and save data file
        iScsiTarget.newLogicalUnit(pdskid, tid, path)
        save_data_file()
    except:
        cleanupPhysicalDisk(pdskid, tid)
        raise

def cleanupPhysicalDisk(pdskid, tid):
    if scan_tid(tid):
        iScsiTarget.delTarget(tid)
    removePdskLink(pdskid)

def scan_tid(tid):
    t = iScsiTarget.getTidTable()
    if t.has_key(tid):
        return True
    return False

def get_tid(pdskid):
    t = iScsiTarget.getTidTable()
    for tid, pdsk in t.iteritems():
        if pdsk == pdskid:
            return tid
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
    t = iScsiTarget.getTidTable()
    for i in range(MIN_TID, MAX_TID):
        if not t.has_key(i):
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

class getDeviceListBadFileException:
    pass

def getDeviceList(path):

    pdsk_re = re.compile("^pdsk-[0-f]+$")
    pdsk_devpath_list = []
    pdskid_list = []

    if path not in (REGISTER_DEVICE_LIST, DEVICE_LIST_FILE):
        raise getDeviceListBadFileException

    if not os.path.exists(path):
        raise getDeviceListBadFileException
    
    f =  open(path, 'r')
    ln = 0
    while True:
        line = f.readline().strip()
        if not line:
            break
        ln += 1
        if line[0] == '#':
            continue
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

def getRoundUpCapacity(capacity):
    unit = min(EXTENTSIZE)
    return ( ( capacity + ( unit - 1 ) ) / unit * unit )

def get_arg_max():
    arg_max_str = executecommand(GETCONF_ARG_MAX)
    return int(arg_max_str, 10)

def getDextDevName(ssvrid, dextid):
    return "%08x-%08x" % (ssvrid, dextid)

def getMirrorDevName(lvolid):
    return "%08x" % (lvolid)

def getLinearDevName(lvolid):
    return "linear-%08x" % (lvolid)

def getMetaDevName(lvolid):
    return "meta-%08x" % (lvolid)

def getDataDevName(lvolid):
    return "data-%08x" % (lvolid)

def getDmDevPath(devname):
    return "%s/%s" % (DM_DEVICE_DIR, devname)

def getDextDevPath(ssvrid, dextid):
    return getDmDevPath(getDextDevName(ssvrid, dextid))

def getMirrorDevPath(lvolid):
    return "%s/%s" % (MD_DEVICE_DIR, getMirrorDevName(lvolid))

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

def notify_sm(method, subargs):
    logger.debug("notify_sm: %s: %s: start" % (method, subargs))
    while True:
        try:
            send_request(host_storage_manager_list, port_storage_manager, \
                method, subargs)
            logger.debug("notify_sm: %s: %s: done" % (method, subargs))
            break
        except xmlrpclib.Fault, inst:
            logger.error("notify_sm: %s: %s: %s" \
                % (method, subargs, os.strerror(inst.faultCode)))
        except Exception, inst:
            logger.error("notify_sm: %s: %s: %s" % \
                (method, subargs, inst))
        time.sleep(SM_DOWN_RETRY_INTARVAL)
        logger.debug("notify_sm: %s: %s retrying..." % (method, subargs))

def dmsetup_remove(devname):
    command = "dmsetup remove %s" % devname
    execute_retry_path_exist(command, getDmDevPath(devname), \
        DMSETUP_RETRY_TIMES)

def get_iscsi_path(ip_addr, pdskid):
    return ISCSI_PATH % (ip_addr, iqn_prefix_iscsi,  pdskid)

def get_ipaddrlist():
    hostname = socket.gethostname()
    list = []
    n = 1
    while True:
        try:
            ip = socket.gethostbyname("%s-data%u" % (hostname, n))
        except:
            if list:
                break
            raise
        list.append(ip)
        n += 1
    return list

def getSnapshotOriginOriginPath(lvolid):
    return "%s/%08x" % (VAS_SNAPSHOT_DIR, lvolid)

def getSnapshotOriginDevName(lvolid):
    return "snapshot-origin-%08x" % lvolid

def blockdev_getsize(path):
    len = 0
    for i in range(0, BLOCKDEV_RETRY_TIMES):
        output = executecommand("blockdev --getsize %s" % path)
        len=int(output)
        if len > 0:
            if i:
                logger.debug("retry OK")
            break
        logger.debug("retrying(%d/%d) ..." % (i + 1, BLOCKDEV_RETRY_TIMES))
        time.sleep(1)
    if len == 0:
        raise Exception, "retry over"
    return len
