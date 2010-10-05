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

__version__ = '$Id: hsvr_agent.py 104 2010-07-21 07:05:47Z yamamoto2 $'

import sys
import getopt
import time
import SimpleXMLRPCServer
import xmlrpclib
import socket
import commands
import os
import signal
import pickle
import subprocess
import traceback
import errno
import threading
from SocketServer import ThreadingMixIn
from vas_conf import *
from vas_subr import lineno, gtos, executecommand, executecommand_retry, \
    dispatch_and_log, get_lvolstruct_of_lvoltype, get_arg_max, \
    getDextDevName, getLinearDevName, getMetaDevName, getDataDevName, \
    getDmDevPath, getDextDevPath, getLinearDevPath, getMetaDevPath, \
    getDataDevPath, getMirrorDevPath, getMultiPathDevice, \
    execute_retry_not_path_exist, execute_retry_path_exist, setupDextDevice, \
    cleanupMultiPathDevice
from vas_db import LVOLTYPE, BIND_STATUS, MIRROR_STATUS
from mdstat import get_rebuild_status

class HsvrAgent:
    def __init__(self):
        self.monitors = {}

    def _dispatch(self, method, params):
        return dispatch_and_log(self, method, params)

    def __loginIscsiTarget(self, lvolstruct_mirrors):
        try:
            paths = []
            paths_2 = []
            paths_for_errout = []
            for mirror in lvolstruct_mirrors:
                if not mirror['lvolspec']['add']:
                    # attachLogicalVolume case
                    for extent in mirror['components']:
                        path = extent['lvolspec']['iscsi_path'][0]
                        path_2 = extent['lvolspec']['iscsi_path'][1]
                        if not os.path.exists(path):
                            if path not in paths:
                                paths.append(path)
                                paths_2.append(path_2)
                else:
                    # replaceMirrorDisk case
                    path = mirror['lvolspec']['add']['lvolspec']['iscsi_path'][0]
                    path_2 = mirror['lvolspec']['add']['lvolspec']['iscsi_path'][1]
                    if not os.path.exists(path):
                        if path not in paths:
                            paths.append(path)
                            paths_2.append(path_2)

            for path in paths:
                i = paths.index(path)
                path_2 = paths_2[i]

                ip_str = path.split(':')[0].split('/dev/disk/by-path/ip-')[1]
                ip_str_2 = path_2.split(':')[0].split('/dev/disk/by-path/ip-')[1]
                pdskid_str = path.split(':')[-1].split('-lun-1')[0]
                executecommand("iscsiadm -m node -o new -T iqn.%s:%s -p %s" % (iqn_prefix_iscsi, pdskid_str, ip_str))
                executecommand("iscsiadm -m node -o new -T iqn.%s:%s -p %s" % (iqn_prefix_iscsi, pdskid_str, ip_str_2))

                command = "iscsiadm -m node -T iqn.%s:%s --login" % (iqn_prefix_iscsi, pdskid_str)
                condition = "not (os.path.exists('"'%s'"') and os.path.exists('"'%s'"'))" % (path, path_2)
                executecommand_retry(command, condition, ISCSIADM_RETRY_TIMES)

                paths_for_errout.append(path)

            time_left = LOGIN_TIMEOUT
            while len(paths) and time_left > 0:
                for path in paths:
                    i = paths.index(path)
                    if os.path.exists(path):
                        try:
                            getMultiPathDevice(path)
                            paths.pop(i)
                        except:
                            executecommand("multipath")
                            time_left -= 1
                            time.sleep(1)
                    else:
                        time_left -= 1
                        time.sleep(1)
        except Exception:
            for path in paths_for_errout:
                try:
                    cleanupMultiPathDevice(path)
                except:
                    # fail through
                    pass
            raise
        return 0

    def __logoutIscsiTarget(self, lvolstruct_mirrors):
        global main_lock
        def check_safe_logout(path):
            devname = os.path.realpath(path).split('/dev/')[1]
            mpaths = os.listdir("/sys/block/%s/holders/" % devname)
            if len(mpaths) != 1:
                # holders exist other than dm_multipath
                return None
            holders_dir = "/sys/block/%s/holders/%s/holders" % (devname, mpaths[0])
            if len(os.listdir(holders_dir)) != 0:
                # holders exist
                return None
            return True

        try:
            main_lock.acquire()
            paths = []
            for mirror in lvolstruct_mirrors:
                if not mirror['lvolspec']['remove']:
                    # detachLogicalVolume case
                    for extent in mirror['components']:
                        path = extent['lvolspec']['iscsi_path'][0]
                        if path not in paths:
                            if check_safe_logout(path):
                                paths.append(path)
                else:
                    # replaceMirrorDisk case
                    path = mirror['lvolspec']['remove']['lvolspec']['iscsi_path'][0]
                    if path not in paths:
                        if check_safe_logout(path):
                            paths.append(path)

            for path in paths:
                cleanupMultiPathDevice(path)

            time_left = LOGIN_TIMEOUT
            while len(paths) and time_left > 0:
                for path in paths:
                    i = paths.index(path)
                    if not os.path.exists(path):
                        paths.pop(i)
                    else:
                        time_left -= 1
                        time.sleep(1)

            main_lock.release()
            return 0
        except Exception:
            main_lock.release()
            raise

    def __invoke_mdadm_monitor_daemon(self, mirrors, lvolid):

        def run_mdadm_monitor(lvolid, targets):
            pid = executecommand("mdadm --monitor --daemonise --program %s %s" % (MDADM_EVENT_CMD, targets))
            self.monitors[lvolid].append(pid)

        # invoke mdadm monitor daemon

        arg_max = get_arg_max()

        bound_mirrors = []
        if self.monitors.has_key(lvolid):
            for mirror in mirrors:
                if mirror['bind_status'] != BIND_STATUS['ALLOCATED']:
                    bound_mirrors.append(mirror)

            self.__kill_mdadm_monitor_daemon(lvolid)

        targets = ""
        self.monitors[lvolid] = []
        for mirror in mirrors:
            targets += "%s " % getMirrorDevPath(mirror['lvolid'])
            if len(targets) > ( arg_max / 2 ):
                run_mdadm_monitor(lvolid, targets)
                targets = ""

        if targets:
            run_mdadm_monitor(lvolid, targets)

        for mirror in bound_mirrors:
            try:
                executecommand("%s RebuildFinished %s/%08x" % (MDADM_EVENT_CMD, MD_DEVICE_DIR, mirror['lvolid']))
            except Exception:
                logger.error(traceback.format_exc())
                # fail through
                pass

    def __kill_mdadm_monitor_daemon(self, lvolid):
        if self.monitors.has_key(lvolid):
            for pid in self.monitors[lvolid]:
                os.kill(int(pid), signal.SIGKILL)
            del self.monitors[lvolid]

    def __blockdev_getsize(self, path):
        mdlen = 0
        for i in range(0, BLOCKDEV_RETRY_TIMES):
            output = executecommand("blockdev --getsize %s" % path)
            mdlen=int(output)
            if mdlen > 0:
                if i:
                    logger.debug("retry OK")
                break
            logger.debug("retrying(%d/%d) ..." % (i + 1, BLOCKDEV_RETRY_TIMES))
            time.sleep(1)

        if mdlen == 0:
            raise Exception, "retry over"

        return mdlen

    def __setup_mirror_devices(self, lvolstruct_mirrors, max_capacity_giga):
        linear = ""
        offset = 0

        global main_lock
        try:
            main_lock.acquire()
            # setup iSCSI devices
            self.__loginIscsiTarget(lvolstruct_mirrors)

            # setup mirror devices
            for mirror in lvolstruct_mirrors:
                mirrordevs = ""
                adddevs = []

                mirrorlvpath = getMirrorDevPath(mirror['lvolid'])
                if os.path.exists(mirrorlvpath):
                    # mirror is already exists
                    if mirror['bind_status'] == BIND_STATUS['ALLOCATED']:
                        logger.info("__setup_mirror_devices: mirror device(%s) already exists. attempt to re-create the device." % mirrorlvpath)
                    else:
                        logger.info("__setup_mirror_devices: mirror device(%s) already exists. attempt to re-assemble the device." % mirrorlvpath)
                    try:
                        executecommand("mdadm -S %s" % mirrorlvpath)
                    except:
                        pass
                    executecommand("rm -f %s" % mirrorlvpath)

                # setup multipath devices
                for extent in mirror['components']:
                    devpath = setupDextDevice(extent)
                    # wait for device ready
                    self.__blockdev_getsize(devpath)
                    if mirror['bind_status'] == BIND_STATUS['ALLOCATED']:
                        # for mdadm --create
                        mirrordevs += " " + devpath 
                    elif extent['bind_status'] == BIND_STATUS['ALLOCATED']:
                        # for mdadm --mannage --add
                        adddevs.append(devpath)
                    else:
                        # for mdadm --assemble
                        mirrordevs += " " + devpath

                mdcommand = ""
                if mirror['bind_status'] == BIND_STATUS['ALLOCATED']:
                    mdcommand = "mdadm --create %s %s --assume-clean --raid-devices=%d %s" % (mirrorlvpath, MDADM_CREATE_OPTIONS, len(mirror['components']), mirrordevs)
                    executecommand(mdcommand)
                else:
                    mdcommand = "mdadm --assemble %s %s %s" % (mirrorlvpath, MDADM_ASSEMBLE_OPTIONS, mirrordevs)
                    executecommand(mdcommand)

                for add in adddevs:
                    executecommand("mdadm --manage --add %s %s" % (mirrorlvpath, add))

                mdlen = self.__blockdev_getsize(mirrorlvpath)
                if max_capacity_giga:
                    limit = gtos(max_capacity_giga)
                    assert(offset < limit), lineno()
                    if offset + mdlen > limit:
                        linear += "%d %d linear %s 0\\n" % (offset, limit - offset, mirrorlvpath)
                        main_lock.release()
                        return (linear, limit)

                linear += "%d %d linear %s 0\\n" % (offset, mdlen, mirrorlvpath)
                offset += mdlen
        except:
            main_lock.release()
            raise

        main_lock.release()
        return (linear, offset)

    def __setup_additional_mirror_devices(self, mirrors):
        linear = ""
        offset = 0

        global main_lock
        try:
            main_lock.acquire()
            # setup iSCSI devices
            self.__loginIscsiTarget(mirrors)

            # setup mirror devices
            for mirror in mirrors:
                mirrordevs = ""

                mirrorlvpath = getMirrorDevPath(mirror['lvolid'])
                if os.path.exists(mirrorlvpath):
                    # mirror is already exists
                    if mirror['bind_status'] == BIND_STATUS['ALLOCATED']:
                        logger.info("__setup_additional_mirror_devices: mirror device(%s) already exists. attempt to re-create the device." % mirrorlvpath)
                        try:
                            executecommand("mdadm -S %s" % mirrorlvpath)
                        except:
                            pass
                        executecommand("rm -f %s" % mirrorlvpath)
                    else:
                        mdlen = self.__blockdev_getsize(mirrorlvpath)
                        linear += "%d %d linear %s 0\\n" % (offset, mdlen, mirrorlvpath)
                        offset += mdlen
                        continue

                assert(mirror['bind_status'] == BIND_STATUS['ALLOCATED']), lineno()

                # setup multipath devices
                for extent in mirror['components']:
                    devpath = setupDextDevice(extent)
                    # wait for device ready
                    self.__blockdev_getsize(devpath)
                    # for mdadm --create
                    mirrordevs += " " + devpath

                mdcommand = "mdadm --create %s %s --assume-clean --raid-devices=%d %s" % (mirrorlvpath, MDADM_CREATE_OPTIONS, len(mirror['components']), mirrordevs)        
                executecommand(mdcommand)

                mdlen = self.__blockdev_getsize(mirrorlvpath)
                linear += "%d %d linear %s 0\\n" % (offset, mdlen, mirrorlvpath)
                offset += mdlen
        except:
            main_lock.release()
            raise

        main_lock.release()
        return (linear, offset)

    def __cleanup_mirror_devices(self, lvolstruct_mirrors):
        for mirror in lvolstruct_mirrors:
            mirrordev = getMirrorDevPath(mirror['lvolid'])
            if os.path.exists(mirrordev):
                executecommand("mdadm -S %s" % mirrordev)
                executecommand("rm -f %s" % mirrordev)
            for extent in mirror['components']:
                devname = getDextDevName(extent['lvolspec']['ssvrid'],extent['lvolid'])
                if os.path.exists(getDmDevPath(devname)):
                    self.__dmsetup_remove(devname)

    def __mdadm_fail_add_remove(self, lvolstruct):
        global main_lock
        try:
            main_lock.acquire()
            # setup iSCSI devices
            self.__loginIscsiTarget([lvolstruct])

            mirrordev = getMirrorDevPath(lvolstruct['lvolid'])
            add = lvolstruct['lvolspec']['add']
            remove = lvolstruct['lvolspec']['remove']

            remove_devname = getDextDevName(remove['lvolspec']['ssvrid'],remove['lvolid'])

            if not os.path.exists(getDmDevPath(remove_devname)):
                raise Exception, "__mdadm_fail_add_remove: %s not exists" % (getDmDevPath(remove_devname))

            executecommand("mdadm --manage --fail %s %s" % (mirrordev, getDmDevPath(remove_devname)))
            add_devpath = setupDextDevice(add)
            # wait for device ready
            self.__blockdev_getsize(add_devpath)
            executecommand("mdadm --manage --add %s %s" % (mirrordev, add_devpath))
            executecommand("mdadm --manage --remove %s %s" % (mirrordev, getDmDevPath(remove_devname)))
            self.__dmsetup_remove(remove_devname)
        except:
            main_lock.release()
            raise
        main_lock.release()

    def __dmsetup_remove(self, devname):
        command = "dmsetup remove %s" % devname
        execute_retry_path_exist(command, getDmDevPath(devname), DMSETUP_RETRY_TIMES)

    def attachLogicalVolume(self, data):
        try:
            lvolstruct = data['lvolstruct']
            lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])
            lvolid_linear = lvolstruct_linear['lvolid']
            lvolstruct_mirrors = lvolstruct_linear['components']
            lvoldev = "%s/%s" % (VAS_DEVICE_DIR, lvolstruct_linear['lvolspec']['lvolname'])

            if os.path.exists(lvoldev):
                # volume is already attached
                return 0

            # invoke mdadm monitor daemon
            self.__invoke_mdadm_monitor_daemon(lvolstruct_mirrors, lvolid_linear)

            # setup mirror devices
	    linear, current_capacity =  self.__setup_mirror_devices(lvolstruct_mirrors, lvolstruct_linear['capacity'])

            # setup linear device
            command = "echo -e \"%s\" | dmsetup create %s" % (linear, getLinearDevName(lvolid_linear))
            execute_retry_not_path_exist(command, getLinearDevPath(lvolid_linear), DMSETUP_RETRY_TIMES)

            # setup symlink to lvoldev
	    srcpath = getLinearDevPath(lvolid_linear)
            os.symlink(srcpath, lvoldev)

        except Exception:
            logger.error(traceback.format_exc())
            try:
                self.__cleanupLogicalVolume(lvolstruct)
            except Exception:
                pass
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def detachLogicalVolume(self, data):
        try:
            self.__cleanupLogicalVolume(data['lvolstruct'])
        except Exception:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0    

    def __cleanupLogicalVolume(self, lvolstruct):
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])
        lvolid_linear = lvolstruct_linear['lvolid']
        lvolstruct_mirrors = lvolstruct_linear['components']
        lvoldev = "%s/%s" % (VAS_DEVICE_DIR, lvolstruct_linear['lvolspec']['lvolname'])

        if os.path.exists(lvoldev):
            os.remove(lvoldev)

        if os.path.exists(getLinearDevPath(lvolid_linear)):
            self.__dmsetup_remove(getLinearDevName(lvolid_linear))

        self.__cleanup_mirror_devices(lvolstruct_mirrors)
        self.__logoutIscsiTarget(lvolstruct_mirrors)
        self.__kill_mdadm_monitor_daemon(lvolid_linear)
            
    def replaceMirrorDisk(self, data):
        try:
            lvolstruct = data['lvolstruct']
            # replace disk extents
            self.__mdadm_fail_add_remove(lvolstruct)
        except Exception:
            logger.error(traceback.format_exc())
            self.__logoutIscsiTarget([lvolstruct])
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            # cleanup unused iSCSI devices
            self.__logoutIscsiTarget([lvolstruct])
        except Exception:
            logger.error(traceback.format_exc())
            # fail through
        return 0

def usage():
    print 'usage: %s [-h|--host] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    global hsvr_agent_object
    for pids in hsvr_agent_object.monitors.values():
        for pid in pids:
            os.kill(int(pid), signal.SIGKILL)
    sys.exit(0)

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():
    global main_lock, hsvr_agent_object
    try:
        opts, _args = getopt.getopt(sys.argv[1:], "h:", ["host=","help"])
    except getopt.GetoptError:
        print "GetoptError"
        usage()
        sys.exit(2)

    host="localhost"

    for o, a in opts:
        if o == "--help":
            usage()
            sys.exit()
        elif o in ("-h", "--host"):
            host = a

    if not os.path.exists(VAS_DEVICE_DIR):
        os.mkdir(VAS_DEVICE_DIR)
    if not os.path.exists(MD_DEVICE_DIR):
        os.mkdir(MD_DEVICE_DIR)

    #daemonize()

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    hsvr_agent_object = HsvrAgent()

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    main_lock = threading.Lock()

    Tserver.allow_reuse_address=True
    server = Tserver((host, port_hsvr_agent))
    server.register_instance(hsvr_agent_object)

    server.register_introspection_functions()

    #Go into the main listener loop
    print "Listening on port %s" % port_hsvr_agent
    server.serve_forever()

if __name__ == "__main__":
    main()
