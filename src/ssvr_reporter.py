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

"""Invoke some storage manager related daemons and register current storage server, disks and a cache device to the storage manager."""
# report local resources to the storage manager.
# start daemons including ssvr_agent.

__version__ = '$Id: ssvr_reporter.py 113 2010-07-23 04:24:12Z yamamoto2 $'

import sys
import os
import getopt
import signal
import re
import pickle
import time
import socket
import traceback
from subprocess import *
from vas_conf import *
from vas_subr import *

def __invoke_daemons():
    executecommand("%s -a %s %s" % (DAEMON_LAUNCHER_CMD, SSVR_AGENT_CMD, SSVR_AGENT_PID))
    executecommand("%s -n 1 %s %s" % (DAEMON_LAUNCHER_CMD, DISKPATROLLER_CMD, DISKPATROLLER_PID))

def __setup_iscsi_targets():
    pdsk_re = re.compile("^pdsk-[0-f]+$")
    for file in os.listdir(STORAGE_MANAGER_VAR):
        if pdsk_re.match(file):
            f = open("%s/%s" % (STORAGE_MANAGER_VAR, file), "r")
            subargs = pickle.load(f)
            f.close()
            activatePhysicalDisk(subargs)

def system_down():
    executecommand(SHUTDOWN_CMD)

def register_resources(refresh):

    ipaddrlist = (socket.gethostbyname(socket.gethostname()+'%s' % DATA1_SUFFIX), socket.gethostbyname(socket.gethostname()+'%s' % DATA2_SUFFIX))

    # check existence of SSVRID_FILE
    initial_setup = True
    if os.path.exists(SSVRID_FILE):
        initial_setup = False

    # check multiple execution
    if os.path.exists(SSVR_REPORTER_PID):
        return
    else:
        f = open(SSVR_REPORTER_PID, "w")
        f.writelines("%d\n" % os.getpid())
        f.close()

    if initial_setup:
        refresh = False

    if not refresh:
        __invoke_daemons()
        __setup_iscsi_targets()

    # [re-]register storage server
    ssvrid = 0
    while ssvrid == 0:
        try:
            if refresh:
                f = open(SSVRID_FILE, "r")
                ssvrid = int(f.readline().rstrip().split('-')[1], 16)
                f.close()
            else:
                subargs = {'ver': XMLRPC_VERSION, 'ip_data': (ipaddrlist[0], ipaddrlist[1])}
                res = send_request(host_storage_manager_list, port_storage_manager, "registerStorageServer", subargs)

                ssvrid = res
                f = open(SSVRID_FILE, "w")
                f.writelines("ssvr-%08x\n" % res)
                f.close()
        except xmlrpclib.Fault, inst:
            if inst.faultCode == errno.EHOSTDOWN:
                # wait for the storage manager to become ready.
                time.sleep(SM_DOWN_RETRY_INTARVAL)
            if inst.faultCode == errno.EEXIST:
                # shutdown to notice the storage manager that a server down is occured.
                logger.error("shutdown")
                system_down()
                raise
        except:
            raise

    # register physical disks
    pdsk_paths = []
    pdskids = []
    new_pdsk_paths = []
    new_pdskids = []

    if initial_setup:
        # get new entries from REGISTER_DEVICE_LIST file
        new_pdsk_paths = getDeviceList(REGISTER_DEVICE_LIST)
    else:
        # get existing entries from device_list file
        pdsk_paths, pdskids = getDeviceList(DEVICE_LIST_FILE)
        if refresh:
            # get new entries from REGISTER_DEVICE_LIST file
            all_pdsk_paths = getDeviceList(REGISTER_DEVICE_LIST)
            for path in all_pdsk_paths:
                if path not in pdsk_paths:
                    new_pdsk_paths.append(path)

    # re-register physical disks
    if pdsk_paths and not refresh:
        for file in pdsk_paths:
            try:
                sectors = getDiskSize(file)
                srp_name = executecommand("cat /proc/scsi_tgt/vdisk/vdisk | grep '%s' | awk '{print $1}'" % ( os.path.realpath(file)) )

                subargs = { \
                'ver': XMLRPC_VERSION, \
                'ssvrid': ssvrid, \
                'capacity': str(sectors), \
                'srp_name': srp_name, \
                'local_path': file, \
                }
                send_request(host_storage_manager_list, port_storage_manager, "registerPhysicalDisk", subargs)
            except xmlrpclib.Fault, inst:
                if inst.faultCode == errno.EEXIST:
                    # shutdown to notice the storage manager that a server down is occured.
                    logger.error("shutdown")
                    system_down()
                raise
            except:
                raise

    # register new physical disks
    if new_pdsk_paths:
        for file in new_pdsk_paths:
            try:
                sectors = getDiskSize(file)
                srp_name = executecommand("cat /proc/scsi_tgt/vdisk/vdisk | grep '%s' | awk '{print $1}'" % ( os.path.realpath(file)) )

                subargs = { \
                'ver': XMLRPC_VERSION, \
                'ssvrid': ssvrid, \
                'capacity': str(sectors), \
                'srp_name': srp_name, \
                'local_path': file, \
                }
                pdskid = send_request(host_storage_manager_list, port_storage_manager, "registerPhysicalDisk", subargs)
                new_pdskids.append(pdskid)
            except:
                # skip errors
                new_pdskids.append(0)
                continue

    pdsks = zip(pdsk_paths, pdskids)
    new_pdsks = zip(new_pdsk_paths, new_pdskids)
    for pdsk in pdsks + new_pdsks:
	path, id = pdsk
	if id == 0:
	    continue
	subargs = { \
	'disk_dev': path, \
	'pdskid': id, \
	'iqn': 'iqn.%s' % (iqn_prefix_iscsi), \
	}
	activatePhysicalDisk(subargs)

    # [re-]generate device_list file
    if initial_setup or refresh:
        f = open(DEVICE_LIST_FILE, 'w')
        for pdsk in pdsks + new_pdsks:
            path, id = pdsk
            if id:
                f.write("pdsk-%08x,%s\n" % (id,path))
        f.close()
        del f

def usage():
    print 'usage: %s [-d|--daemonize] [-r|--refresh] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    if os.path.exists(SSVR_REPORTER_PID):
        os.remove(SSVR_REPORTER_PID)
    sys.exit(0)

def main():
    try:
        opts, _args = getopt.getopt(sys.argv[1:], "dr", ["daemonize","refresh","help"])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    refresh = False
    for o, _a in opts:
        if o == "--help":
            usage()
            sys.exit()
        elif o in ("-d", "--daemonize"):
            daemonize()
        if o in ("-r", "--refresh"):
            refresh = True

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    try:
        register_resources(refresh)
        os.remove(SSVR_REPORTER_PID)
    except:
        logger.error(traceback.format_exc())
        if os.path.exists(SSVR_REPORTER_PID):
            os.remove(SSVR_REPORTER_PID)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
