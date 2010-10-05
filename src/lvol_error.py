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

import sys
import os
import re
import time
import signal
import vas_db
import mdstat
from vas_conf import *
from vas_subr import *

lvol_re = re.compile("^lvol-[0-f]+$")

def main():
    while True:
        time.sleep(LVOL_ERROR_INTERVAL)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        linear_list = []
        for dent in os.listdir('/dev/mapper/'):
            if lvol_re.match(dent):
                linear_list.append(dent)

        faulty_list = []
        for linear in linear_list:
            try:
                deps, _, _, _ = mdstat.getMirrorList(linear, {}, {})
            except:
                logger.error("getMirrorList failed: lvol %s" % (linear))
                continue
            logger.debug('deps %s' % (deps))
            for md in deps.values():
                try:
                    dexts, _ = mdstat.getDextList(md, {})
                except:
                    logger.error("getDextList failed: md %s" % (md))
                    continue
                logger.debug('dexts in %s: %s' % (md, dexts))
                for dext in dexts:
                    if dext['dext_status'] == 'faulty':
                        faulty_list.append(dext['dextid'])

        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        for lvol in faulty_list:
            pid = os.fork()
            if pid != 0:
                continue
            try:
                subargs = {'ver': XMLRPC_VERSION, 'target': vas_db.TARGET['LVOL'], 'targetid': lvol}
                send_request(host_storage_manager_list, port_storage_manager, "notifyFailure", subargs)
            except:
                logger.error("notifyFailure LVOL %08x was called, but got an error." % (lvol))
            else:
                logger.info("notifyFailure LVOL %08x was called" % (lvol))
            os._exit(0)

if __name__ == '__main__':
    main()
