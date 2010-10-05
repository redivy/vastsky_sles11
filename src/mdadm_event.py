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

__version__ = '$Id: mdadm_event.py 24 2010-07-05 02:58:29Z yamamoto2 $'

import sys
import string
import xmlrpclib
import socket
import time
import os
import traceback
from vas_conf import *
from vas_subr import *
from mdstat import get_rebuild_status

def main():
    if len(sys.argv) != 3 or sys.argv[1] != "RebuildFinished":
        sys.exit(0)

    dir = MD_DEVICE_DIR + "/"
    mirrorid = int(string.replace(sys.argv[2], dir, ''), 16)
    data = {'ver': XMLRPC_VERSION, 'mirrorid': mirrorid, 'dexts': get_rebuild_status(mirrorid)}

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    try:
        while True:
            try:
                send_request(host_storage_manager_list, port_storage_manager, "notifyRebuildMirrorFinished", data)
                break
            except xmlrpclib.Fault, inst:
                if inst.faultCode == errno.EHOSTDOWN:
                    # wait for the storage manager to become ready.
                    logger.info("notifyRebuildMirrorFinished: EHOSTDOWN. retrying ...")
                    time.sleep(SM_DOWN_RETRY_INTARVAL)
                    continue
                else:
                    raise Exception, "notifyRebuildMirrorFinished: lvol-%08x: %s" % (mirrorid, os.strerror(inst.faultCode))
            except Exception, inst:
                raise Exception, "notifyRebuildMirrorFinished: lvol-%08x: %s" % (mirrorid, inst)
    except Exception, inst:
        logger.error(traceback.format_exc())
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
