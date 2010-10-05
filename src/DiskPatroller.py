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
import subprocess
import time
from vas_conf import *
from vas_subr import *
from vas_db import *

device_list = {} # 'devname': offset
if SCRUB_EXTENT_SIZE % SCRUB_STRIPE_SIZE != 0:
    raise Exception("SCRUB_EXTENT_SIZE not muktiple of SCRUB_STRIPE_SIZE")
max_off = SCRUB_EXTENT_SIZE / SCRUB_STRIPE_SIZE

def should_scrub(dev):
    try:
        int(dev, 16)
    except ValueError, e:
        return False
    else:
        return True

def update_device_list():
    try:
        li = os.listdir(VAS_PDSK_DIR)
    except:
        logger.error("listdir %s failed." % VAS_PDSK_DIR)
        sys.exit(1)
    for device in li:
        if should_scrub(device) is True and not device_list.has_key(device):
            logger.debug('new device %s found' % (device))
            device_list[device] = 0
    for device in device_list.keys():
        if not device in li:
            logger.debug('%s was removed' % device)
            del device_list[device]
    logger.debug('device list update: %s' % (device_list))

def scrub(device):
    off = device_list[device]
    device_list[device] += 1
    if device_list[device] == max_off:
        device_list[device] = 0
    dev_path = '%s/%s' % (VAS_PDSK_DIR, device)
    extent = 0
    while True:
        try:
            fd = os.open(dev_path, os.O_RDONLY)
        except OSError:
            logger.exception('error: couldn\'t open %s' % (dev_path))
            return
        offset = max_off * extent + off
        try:
            os.lseek(fd, offset * SCRUB_STRIPE_SIZE * 1024 * 1024, 0)
        except OSError: # OSError: [Errno 22] Invalid argument
            logger.debug('%s: end of the device' % (device))
            os.close(fd)
            return
        except:
            logger.error("open %s unknown error" % dev_path)
            os.close(fd)
            return # ignore
        os.close(fd)
        cmdline = ['/bin/dd', 'if=%s' % (dev_path), 'of=/dev/null', 'iflag=direct', 'bs=%sM' % (SCRUB_STRIPE_SIZE), 'count=1', 'skip=%s' % (offset)]
        logger.debug(cmdline)
        try:
            sub = subprocess.Popen(cmdline, stdin = subprocess.PIPE, stdout = subprocess.PIPE, \
            stderr = subprocess.PIPE)
        except OSError:
            pass
        else:
            stdout, stderr = sub.communicate(None)
            ret = sub.returncode
            logger.debug('Device %s, stderr: %s' % (dev_path, stderr))
            logger.debug('Device %s, ret: %s' % (dev_path, ret))
            if ret > 0:
		# XXX should use notifyBadBlocks?
                pdskid = int(device, 16)
                logger.info("notifyFailure for pdskid %s" % pdskid);
                subargs = {'ver': XMLRPC_VERSION, \
                    'target': TARGET['PDSK'], 'targetid': pdskid}
                while True:
                    try:
                        send_request(host_storage_manager_list, \
                            port_storage_manager, "notifyFailure", subargs)
                        break
                    except xmlrpclib.Fault, inst:
                        if inst.faultCode == errno.EHOSTDOWN:
                            # wait for the storage manager to become ready.
                            logger.info( \
                                "notifyFailure: EHOSTDOWN. retrying ...")
                            time.sleep(SM_DOWN_RETRY_INTARVAL)
                            continue
                        else:
                            logger.info( \
                                "notifyFailure: %d. ignore." % inst.faultCode)
                            break
                    except Exception, inst:
                        logger.info("notifyFailure: %s. ignore." % inst)
                        break
        time.sleep(SCRUB_SLEEP_TIME)
        extent += 1

def main():
    time.sleep(SCRUB_FIRST_SLEEP_TIME)
    while True:
        update_device_list()
        for device in device_list.keys():
            scrub(device)

if __name__ == '__main__':
    main()
