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

__version__ = '$Id: ssvr_agent.py 120 2010-07-26 03:14:41Z yamamoto2 $'

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
from vas_subr import vbtos, stovb, execute_retry_not_path_exist, \
    dispatch_and_log, send_request
import vas_db

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

def do_shred(path, offset_mb, len_mb, lvolid):
    # zero clear disk extent
    command = '%s if=/dev/zero of=%s bs=1024k seek=%d count=%d %s' \
	% (DD_CMD, path, offset_mb, len_mb, DD_OPTIONS)
    try:
        status = subprocess.call(command.split(' '))
    except OSError, e:
        logger.error("exec failed '%s' %s" % (command, e))
	# XXX what to do?
	status = -1
    logger.debug("do_shred: %s status=%d" % (command, status))
    if status and not IGNORE_SHRED_STATUS:
	notify_sm("notifyFailure", \
	    {'ver': XMLRPC_VERSION, 'target': vas_db.TARGET['LVOL'], \
	    'targetid': lvolid})
    notify_sm("notifyShredFinished", \
	{'ver': XMLRPC_VERSION, 'dextid': lvolid})

shredder_cv = threading.Condition()
shredder_queue = []

class Shredder(threading.Thread):
    def run(self):
        global shredder_queue, shredder_cv
        logger.info("shredder started")
        while True:
	    shredder_cv.acquire()
            while not shredder_queue:
		logger.info("shredder sleeping")
		shredder_cv.wait()
	    w = shredder_queue.pop()
	    shredder_cv.release()
            logger.info("shredder woken %s", w)
	    do_shred(w['local_path'], w['offset_mb'], w['len_mb'], w['lvolid'])

class SsvrAgent:
    def __init__(self):
        pass

    def _dispatch(self, method, params):
        return dispatch_and_log(self, method, params)

    def registerShredRequest(self,data):
	global shredder_queue, shredder_cv
	def vbtomb(vb):
	    mb = vbtos(vb) / (1024 * 1024 / 512)
	    tvb, todd = stovb(mb * (1024 * 1024 / 512))
	    if tvb != vb or todd != 0:
		logger.info("vb %s != %s (%s)" % \
		    (stovb(mb * (1024 * 1024 / 512)), vb, mb))
		raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
	    return mb
        try:
            local_path = '%s/%08x' % (VAS_PDSK_DIR, data['pdskid'])
	    # XXX wait for ssvr_reporter creating symlinks.
	    execute_retry_not_path_exist("false", local_path, 16)
            w = { 'local_path' : local_path,
                'offset_mb' : vbtomb(data['offset']),
                'len_mb' : vbtomb(data['capacity']),
		'lvolid' : data['dextid'] }
            shredder_cv.acquire()
            shredder_queue.append(w)
            shredder_cv.notify()
            shredder_cv.release()
	except xmlrpclib.Fault:
	    raise
        except Exception:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

def usage():
    print 'usage: %s [-h|--host] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    os.killpg(0, signum)
    sys.exit(0)

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():

    logger.info("starting: $Id: ssvr_agent.py 120 2010-07-26 03:14:41Z yamamoto2 $")
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

    logger.info("starting shredders")
    for i in range(SHREDDER_COUNT):
        s = Shredder()
        s.setDaemon(True)
	s.start()

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    ssvr_agent_object = SsvrAgent()

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    logger.info("creating Tserver on port %s" % port_ssvr_agent)
    Tserver.allow_reuse_address = True
    server = Tserver((host, port_ssvr_agent))
    logger.info("register inst")
    server.register_instance(ssvr_agent_object)
    logger.info("register intro")
    server.register_introspection_functions()

    #Go into the main listener loop
    logger.info("Listening on port %s" % port_ssvr_agent)
    server.serve_forever()

if __name__ == "__main__":
    main()
