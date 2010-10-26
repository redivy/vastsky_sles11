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

__version__ = '$Id$'

import threading
import traceback
from vas_conf import logger
from vas_subr import __reverse_dict

EVENT_STATUS = {'NONE': 0, 'PENDING': 1, 'PROGRESS': 2, 'DONE': 3, 'CANCELED': 4, 'ERROR': 5}
EVENT = {'ABNORMAL':1, 'RESYNC':2, 'SHRED': 3}
EVENT_STATUS_STR = __reverse_dict(EVENT_STATUS)
EVENT_STR = __reverse_dict(EVENT)

class EventRecorder:
    class Event:
        def __init__(self, eventid, status, result = 0, func = None, args = None):
            self.eventid = eventid
            self.status = status
            self.result = result
            self.func = func
            self.args = args

    def __init__(self):
        self.event_lock = threading.Lock()
        self.event_record = {}

    def lock(self, needlock = True):
        if needlock:
            self.event_lock.acquire()

    def unlock(self, needlock = True):
        if needlock:
            self.event_lock.release()

    def event_exist(self, eventid, needlock = False):
        self.lock(needlock)
        ret = False
        if self.event_record.has_key(eventid):
            ret = True
        self.unlock(needlock)
        return ret

    def del_event(self, eventid, needlock = False):
        self.lock(needlock)
        assert(self.event_record.has_key(eventid)), self.unlock(needlock)
        del self.event_record[eventid]
        self.unlock(needlock)

    def add_event(self, eventid, status, result = 0, func = None, args = None, needlock = False):
        self.lock(needlock)
        assert(not self.event_record.has_key(eventid)), self.unlock(needlock)
        self.event_record[eventid] = self.Event(eventid, status, result, func, args)
        self.unlock(needlock)
        
    def get_event(self, eventid, needlock = False):
        self.lock(needlock)
        assert(self.event_record.has_key(eventid)), self.unlock(needlock)
        e = self.event_record[eventid]
        self.unlock(needlock)
        return e

    def get_event_status(self, eventid, needlock = False):
        self.lock(needlock)
        assert(self.event_record.has_key(eventid)), self.unlock(needlock)
        st = self.event_record[eventid].status
        self.unlock(needlock)
        return st

    def set_event_status_and_result(self, eventid, status, result, needlock = False):
        self.lock(needlock)
        assert(self.event_record.has_key(eventid)), self.unlock(needlock)
        e = self.event_record[eventid]
        e.status = status
        e.result = result
        self.unlock(needlock)
       
def get_event_status(record, eventid):
    def normal_check():
        if not record.event_exist(eventid):
            return (EVENT_STATUS['NONE'], 0)
        e = record.get_event(eventid)
        ret = (e.status, e.result)
        if e.status in (EVENT_STATUS['DONE'], EVENT_STATUS['ERROR']):
            record.del_event(eventid)
        return ret

    record.lock()
    try:
        st, re = normal_check()
        if st != EVENT_STATUS['PROGRESS']:
            record.unlock()
            return (st, re)
        e = record.get_event(eventid)
        if e.func:
            e.func(eventid, *e.args)
            # event record may be changed. get again.
            st, re = normal_check()
        record.unlock()
        return (st, re)
    except Exception:
        record.unlock()
        logger.error(traceback.format_exc())
        raise

