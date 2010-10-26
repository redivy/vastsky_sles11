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

__version__ = '$Id: ssvr_agent.py 321 2010-10-20 05:59:06Z yamamoto2 $'

import errno
import sys
import os
import getopt
import signal
import subprocess
import SimpleXMLRPCServer
import xmlrpclib
import socket
import traceback
import threading
from SocketServer import ThreadingMixIn
from vas_conf import *
from vas_subr import dispatch_and_log, execute_retry_not_path_exist, \
    executecommand, mand_keys, notify_sm, stovb, vbtos
from vas_const import TARGET
from event import EventRecorder, get_event_status, EVENT_STATUS

class SsvrAgent:
    def __init__(self):
        self.event_record = EventRecorder()

    def _dispatch(self, method, params):
        return dispatch_and_log(self, method, params)

    def __do_shred(self, eventid, path, offset_mb, len_mb, dextid, pdskid):
        # zero clear disk extent
        command = '%s if=/dev/zero of=%s bs=1024k seek=%d count=%d %s' \
            % (DD_CMD, path, offset_mb, len_mb, DD_OPTIONS)
        try:
            executecommand(command)
        except Exception, e:
            logger.error("do_shred: %s" % (e))
            self.event_record.del_event(eventid, needlock = True)
# bug 3082606
#           notify_sm("notifyFailure", \
#               {'ver': XMLRPC_VERSION, 'target': TARGET['LVOL'], \
#               'targetid': dextid})
            notify_sm("notifyFailure", \
                {'ver': XMLRPC_VERSION, 'target': TARGET['PDSK'], \
                'targetid': pdskid})
            return
        logger.debug("shred done %d %d" % (eventid, dextid))
        self.event_record.set_event_status_and_result(eventid, \
            EVENT_STATUS['DONE'], 0, needlock = True)
        return

    def __start_shredder(self, eventid, path, offset_mb, len_mb, dextid, \
        pdskid):
        th = threading.Thread(target = self.__do_shred, \
            args = (eventid, path, offset_mb, len_mb, dextid, pdskid))
        th.setDaemon(True)
        th.start()

    def registerShredRequest(self, data):
        def vbtomb(vb):
            mb = vbtos(vb) / (1024 * 1024 / 512)
            tvb, todd = stovb(mb * (1024 * 1024 / 512))
            if tvb != vb or todd != 0:
                logger.info("vb %s != %s (%s)" % \
                    (stovb(mb * (1024 * 1024 / 512)), vb, mb))
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            return mb

        self.event_record.lock()
        try:
            eventid, dextid, pdskid, offset, capacity = \
                mand_keys(data, 'eventid', 'dextid', 'pdskid', 'offset', \
                'capacity')
            if self.event_record.event_exist(eventid):
                self.event_record.unlock()
                return 0
            local_path = '%s/%08x' % (VAS_PDSK_DIR, pdskid)
            # XXX wait for ssvr_reporter creating symlinks.
            execute_retry_not_path_exist("false", local_path, 16)
            self.__start_shredder(eventid, local_path, vbtomb(offset), \
                vbtomb(capacity), dextid, pdskid)
            self.event_record.add_event(eventid, EVENT_STATUS['PROGRESS'])
            self.event_record.unlock()
        except xmlrpclib.Fault:
            self.event_record.unlock()
            raise
        except Exception:
            self.event_record.unlock()
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def getEventStatus(self, data):
        eventid = mand_keys(data, 'eventid')
        try:
            return get_event_status(self.event_record, eventid)
        except Exception:
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        
def usage():
    print 'usage: %s [-h|--host] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    os.killpg(0, signum)
    sys.exit(0)

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():

    logger.info("starting: $Id: ssvr_agent.py 321 2010-10-20 05:59:06Z yamamoto2 $")
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

    #daemonize()

    os.setpgid(0, 0)

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    ssvr_agent_object = SsvrAgent()

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    logger.debug("creating Tserver on port %s" % port_ssvr_agent)
    Tserver.allow_reuse_address = True
    server = Tserver((host, port_ssvr_agent))
    logger.debug("register inst")
    server.register_instance(ssvr_agent_object)
    logger.debug("register intro")
    server.register_introspection_functions()

    #Go into the main listener loop
    logger.debug("Listening on port %s" % port_ssvr_agent)
    server.serve_forever()

if __name__ == "__main__":
    main()
