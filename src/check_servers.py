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

# check head/storage server agents' responsiveness by calling helloWorld
# method and report unresponsive ones to the storage manager via notifyFalure
# method.

__version__ = '$Id: check_servers.py 319 2010-10-20 05:54:09Z yamamoto2 $'

import os
import sys
import time
import signal
from vas_const import TARGET, ALLOC_PRIORITY_STR
from vas_conf import *
from vas_subr import *

retry_check_interval = 10 # seconds
retry_check_num = 1

def call_agent(ipaddr_list, port):
    attempt = 0
    while True:
        try:
            subargs = {'ver': XMLRPC_VERSION}
            send_request(ipaddr_list, port, "helloWorld", subargs)
        except:
            if attempt < retry_check_num:
                attempt += 1
                time.sleep(retry_check_interval)
                continue
            else:
                raise
        else:
            return # alive
    
def check_agent(type, id, ipaddr_list):
    if type == "hsvr":
        port = port_hsvr_agent
        target = TARGET['HSVR']
    else: # "ssvr"
        port = port_ssvr_agent
        target = TARGET['SSVR']

    pid = os.fork()
    if pid != 0:
        return
    try:
        call_agent(ipaddr_list, port)
    except:
        logger.info("%s %s %s does not respond" % (type, id, ipaddr_list))
        try:
            subargs = {'ver': XMLRPC_VERSION, 'target': target, 'targetid': id}
            res = send_request(["localhost"], port_storage_manager, "notifyFailure", subargs)
        except:
            logger.info("notifyFailure %s %s was called, but got an error." % (type, id))
    os._exit(0)

def main():
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    while True:
        time.sleep(SERVER_CHECK_INTERVAL)
        try:
            subargs = {'ver': XMLRPC_VERSION}
            head_servers = send_request(["localhost"], port_storage_manager, "listHeadServers", subargs)
            subargs = {'ver': XMLRPC_VERSION}
            storage_servers = send_request(["localhost"], port_storage_manager, "listStorageServers", subargs)
        except:
            logger.info("Couldn't get listHeadServers or listStorageServers")
            continue

        for head in head_servers:
            if ALLOC_PRIORITY_STR[head['priority']] != 'HIGH':
                continue
            check_agent("hsvr", head['hsvrid'], head['ip_data'])

        for storage in storage_servers:
            if ALLOC_PRIORITY_STR[storage['priority']] != 'HIGH':
                continue
            check_agent("ssvr", storage['ssvrid'], storage['ip_data'])

if __name__ == '__main__':
    main()
