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

"""Invoke some storage manager related daemons and register current head server and a cache device to the storage manager."""
# report local resources to the storage manager.
# start daemons including hsvr_agent.

__version__ = '$Id: hsvr_reporter.py 104 2010-07-21 07:05:47Z yamamoto2 $'

import sys
import os
import getopt
import signal
import time
import socket
import traceback
from subprocess import *
from vas_conf import *
from vas_subr import *
from vas_db import TARGET

def __invoke_daemons():
    executecommand("%s -a %s %s" % (DAEMON_LAUNCHER_CMD, HSVR_AGENT_CMD, HSVR_AGENT_PID))
    executecommand("%s -n 1 %s %s" % (DAEMON_LAUNCHER_CMD, LVOL_ERROR_CMD, LVOL_ERROR_PID))

def register_resources():

    ipaddrlist = (socket.gethostbyname(socket.gethostname()+'%s' % DATA1_SUFFIX), socket.gethostbyname(socket.gethostname()+'%s' % DATA2_SUFFIX))

    # check existence of HSVRID_FILE
    initial_setup = True
    my_hsvrid = 0
    if os.path.exists(HSVRID_FILE):
        initial_setup = False
        f = open(HSVRID_FILE, "r")
        my_hsvrid = int(f.readline().rstrip().split('-')[1], 16)
        f.close()

    # check multiple execution
    if os.path.exists(HSVR_REPORTER_PID):
        return
    else:
        f = open(HSVR_REPORTER_PID, "w")
        f.writelines("%d\n" % os.getpid())
        f.close()

    __invoke_daemons()

    # [re]register head server
    hsvrid = 0
    while hsvrid == 0:
        try:
            subargs = {'ver': XMLRPC_VERSION, 'ip_data': (ipaddrlist[0], ipaddrlist[1])}
            res = send_request(host_storage_manager_list, port_storage_manager, "registerHeadServer", subargs)

            hsvrid = res
            f = open(HSVRID_FILE, "w")
            f.writelines("hsvr-%08x\n" % res)
            f.close()
        except xmlrpclib.Fault, inst:
            if inst.faultCode == errno.EHOSTDOWN:
                # wait for the storage manager to become ready.
                time.sleep(SM_DOWN_RETRY_INTARVAL)
                continue
            if inst.faultCode == errno.EEXIST:
                if not initial_setup:
                    subargs = {'ver': XMLRPC_VERSION, 'target': TARGET['HSVR'], 'targetid': my_hsvrid}
                    send_request(host_storage_manager_list, port_storage_manager, "notifyFailure", subargs)
                    continue
                else:
                    logger.error("registerHeadServer: %s: %s" % (ipaddrlist, os.strerror(inst.faultCode)))
                    raise
            else:
                logger.error("registerHeadServer: %s: %s" % (ipaddrlist, os.strerror(inst.faultCode)))
                raise
        except:
            raise

def usage():
    print 'usage: %s [-d|--daemonize] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    if os.path.exists(HSVR_REPORTER_PID):
        os.remove(HSVR_REPORTER_PID)
    sys.exit(0)

def main():
    try:
        opts, _args = getopt.getopt(sys.argv[1:], "d", ["daemonize","help"])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for o, _a in opts:
        if o == "--help":
            usage()
            sys.exit()
        elif o in ("-d", "--daemonize"):
            daemonize()

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    try:
        register_resources()
        os.remove(HSVR_REPORTER_PID)
    except:
        logger.error(traceback.format_exc())
        if os.path.exists(HSVR_REPORTER_PID):
            os.remove(HSVR_REPORTER_PID)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
