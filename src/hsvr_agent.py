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

__version__ = '$Id: hsvr_agent.py 319 2010-10-20 05:54:09Z yamamoto2 $'

import sys
import getopt
import time
import SimpleXMLRPCServer
import xmlrpclib
import socket
import os
import signal
import traceback
import errno
import threading
import dag
import worker
import mynode
import hsvr_dag

from SocketServer import ThreadingMixIn
from vas_conf import *
from vas_subr import lineno, executecommand, dispatch_and_log, \
    getDextDevPath, getMirrorDevPath, mand_keys, start_worker
from event import EventRecorder, get_event_status, EVENT_STATUS
from lvol_error import get_mirror_status

def mydaglogger(n, e, u):
    if u:
        logger.debug("DAG: %s %s (undo)\n" % (n, e))
    else:
        logger.debug("DAG: %s %s\n" % (n, e))

def noop(n):
    pass

def get_lvol_path_from_lvolstruct(lvolstruct):
    return "%s/%s" % (VAS_DEVICE_DIR, lvolstruct['lvolspec']['lvolname'])

class HsvrAgent:
    def __init__(self):
        self.dag_worker = worker.Worker(16)
        self.event_record = EventRecorder()

    def _dispatch(self, method, params):
        return dispatch_and_log(self, method, params)

    def attachLogicalVolume(self, data):
        eventid, lvolstruct = mand_keys(data, 'eventid', 'lvolstruct')
        self.event_record.lock()
        if self.event_record.event_exist(eventid):
            self.event_record.unlock()
            return 0
        start_worker(self.__attach_worker, eventid, lvolstruct)
        self.event_record.add_event(eventid, EVENT_STATUS['PROGRESS'])
        self.event_record.unlock()
        return 0

    def __attach_worker(self, eventid, lvolstruct):
        failed = []
        try:
            nodes = hsvr_dag.create_dag_from_lvolstruct(lvolstruct, False)
            failed = dag.dag_execute(nodes, mynode.mynode_do, \
                mynode.mynode_undo, self.dag_worker, mydaglogger)
            for n, ei in failed:
                t, v, tr = ei
                raise t, v
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['DONE'],\
                0, needlock = True)
        except Exception:
            if failed:
                for n, ei in failed:
                    t, v, tr = ei
                    logger.error(traceback.format_exception(t, v, tr))
            else:
                logger.error(traceback.format_exc())
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['ERROR'], \
                500, needlock = True)
        return 0

    def detachLogicalVolume(self, data):
        eventid, lvolstruct = mand_keys(data, 'eventid', 'lvolstruct')
        self.event_record.lock()
        if self.event_record.event_exist(eventid):
            self.event_record.unlock()
            return 0
        start_worker(self.__detach_worker, eventid, lvolstruct)
        self.event_record.add_event(eventid, EVENT_STATUS['PROGRESS'])
        self.event_record.unlock()
        return 0

    def __detach_worker(self, eventid, lvolstruct):
        failed = []
        try:
            nodes = hsvr_dag.create_dag_from_lvolstruct(lvolstruct, True)
            failed = dag.dag_execute(nodes, mynode.mynode_undo, noop, \
                self.dag_worker, mydaglogger)
            for n, ei in failed:
                t, v, tr = ei
                raise t, v
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['DONE'], \
               0, needlock = True)
        except xmlrpclib.Fault, inst:
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['ERROR'], \
               inst.faultCode, needlock = True)
        except Exception, inst:
            if failed:
                for n, ei in failed:
                    t, v, tr = ei
                    logger.error(traceback.format_exception(t, v, tr))
            else:
                logger.error(traceback.format_exc())
            if os.path.exists(get_lvol_path_from_lvolstruct(lvolstruct)):
                # symlink exists, so nothing done. 
                # return EBUSY in this case acordings to the protocol between SM
                self.event_record.set_event_status_and_result(eventid, \
                    EVENT_STATUS['ERROR'], errno.EBUSY, needlock = True)
            else:
                self.event_record.set_event_status_and_result(eventid, \
                    EVENT_STATUS['ERROR'], 500, needlock = True)
        return 0    

    def replaceMirrorDisk(self, data):
        eventid, mirrorid, add_lvolstruct, remove_lvolstruct = \
            mand_keys(data, 'eventid', 'mirrorid', 'add', 'remove')
        self.event_record.lock()
        if self.event_record.event_exist(eventid):
            self.event_record.unlock()
            return 0
        dextid = add_lvolstruct['lvolid']
        status = get_mirror_status(mirrorid)
        if not status: # mirror not exist
            self.event_record.unlock()
            raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
        if not status.has_key(dextid) or status[dextid] == "faulty": # start rebuild
            start_worker(self.__mirror_worker, eventid, mirrorid, add_lvolstruct, remove_lvolstruct)
            self.event_record.add_event(eventid, EVENT_STATUS['PROGRESS'], 0, \
                self.__check_mirror_status, (mirrorid, add_lvolstruct['lvolid']))
        elif status[dextid] == "in_sync": # rebuild done already
            self.event_record.add_event(eventid, EVENT_STATUS['DONE'], 0)
        elif status[dextid] == "spare": # rebuild started already
            self.event_record.add_event(eventid, EVENT_STATUS['PROGRESS'], 0, \
                self.__check_mirror_status, (mirrorid, add_lvolstruct['lvolid']))
        # else: that's all
        self.event_record.unlock()
        return 0

    def __check_mirror_status(self, eventid, mirrorid, dextid):
        # this method called with event_record locked
        try:
            status = get_mirror_status(mirrorid)
            if not status or not status.has_key(dextid): # mirror or dext gone ?
                self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['ERROR'], errno.ENOENT)
            elif status[dextid] == 'in_sync': # done
                self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['DONE'], 0)
            # else: rebuild progress, status not change
        except Exception:
            logger.error(traceback.format_exc())
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['ERROR'], 500)

    def __mirror_worker(self, eventid, mirrorid, add_lvolstruct, remove_lvolstruct):
        class MdadmNode(mynode.MyNode):
            def __init__(self, op, mirrordev, component):
                self.md_op = op
                self.md_mirrordev = mirrordev
                self.md_component = component
                mynode.MyNode.__init__(self, "mdadm:%s:%s:%s" % \
                    (op, mirrordev, component))
            def do(self):
                try:
                    executecommand("mdadm --manage --%s %s %s" % \
                        (self.md_op, self.md_mirrordev, \
                        self.md_component))
                except:
                    if self.md_op == "fail" or self.md_op == "remove":
                        # pass through dext remove.
                        # if fail/remove fail by already removed, it is OK and dext remove should be OK.
                        # otherwise dext remove will fail and return error to the caller
                        return
                    else:
                        raise
        def get_dext_path_from_lvolstruct(l):
            return getDextDevPath(l['lvolspec']['ssvrid'], l['lvolid'])
        failed = []
        try:
            mirrordev = getMirrorDevPath(mirrorid)
            add_path = get_dext_path_from_lvolstruct(add_lvolstruct)
            mdadd_node = MdadmNode("add", mirrordev, add_path)
            add_nodes = hsvr_dag.create_dag_from_lvolstruct(add_lvolstruct, \
                False)
            for n in add_nodes:
                mdadd_node.add_antecedent(n)
            remove_nodes = []
            if remove_lvolstruct['lvolid'] != 0:
                remove_path = get_dext_path_from_lvolstruct(remove_lvolstruct)
                if os.path.exists(remove_path):
                    mdfail_node = MdadmNode("fail", mirrordev, remove_path)
                    mdremove_node = MdadmNode("remove", mirrordev, remove_path)
                else:
                    mdfail_node = mynode.MyNode("mdfail_dummy")
                    mdremove_node = mynode.MyNode("mdremove_dummy")
                mdremove_node.add_antecedent(mdfail_node)
                if remove_lvolstruct['lvolid'] != add_lvolstruct['lvolid']:
                    remove_nodes = hsvr_dag.create_dag_from_lvolstruct( \
                        remove_lvolstruct, True)
                    for n in remove_nodes:
                        n.add_antecedent(mdremove_node)
                nodes = add_nodes + remove_nodes + \
                    [mdfail_node, mdadd_node, mdremove_node]
                mdadd_node.add_antecedent(mdremove_node)
            else: # add only
                nodes = add_nodes + [mdadd_node]
            def replace_do(n):
                if not n in remove_nodes:
                    mynode.mynode_do(n)
                else:
                    mynode.mynode_undo(n)
            def replace_undo(n):
                if not n in remove_nodes:
                    mynode.mynode_undo(n)
            failed = dag.dag_execute(nodes, replace_do, replace_undo, \
                self.dag_worker, mydaglogger)
            for n, ei in failed:
                t, v, tr = ei
                raise t, v
        except Exception:
            self.event_record.set_event_status_and_result(eventid, EVENT_STATUS['ERROR'], \
                500, needlock = True)
            if failed:
                for n, ei in failed:
                    t, v, tr = ei
                    logger.error(traceback.format_exception(t, v, tr))
            logger.error(traceback.format_exc())
        return 0

    def getEventStatus(self, data):
        eventid = mand_keys(data, 'eventid')
        try:
            return get_event_status(self.event_record, eventid)
        except Exception:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

def usage():
    print 'usage: %s [-h|--host] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    sys.exit(0)

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():
    global hsvr_agent_object
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

    Tserver.allow_reuse_address=True
    server = Tserver((host, port_hsvr_agent))
    server.register_instance(hsvr_agent_object)

    server.register_introspection_functions()

    #Go into the main listener loop
    print "Listening on port %s" % port_hsvr_agent
    server.serve_forever()

if __name__ == "__main__":
    main()
