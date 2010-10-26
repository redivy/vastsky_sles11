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

__version__ = '$Id: worker.py 252 2010-09-16 07:07:29Z yamamoto2 $'

import threading
from collections import deque

# it's important to ensure that worker threads do not have
# a reference to the Worker instance.  otherwise, Worker.__del__
# is not called.  it's one of the reasons why WorkQueue is separated
# from Worker.

class Worker(object):
    class WorkQueue(object):
        def __init__(self):
            self.stopping = False
            self.queue = deque()
            self.lock = threading.Lock()
            self.cv = threading.Condition(self.lock)
            self.draincv = threading.Condition(self.lock)
        def execute(self):
            while True:
                self.lock.acquire()
                while True:
                    if self.stopping:
                        self.lock.release()
                        return
                    if self.queue:
                        break;
                    self.draincv.notify()
                    self.cv.wait()
                _, f = self.queue.popleft()
                self.lock.release()
                f()
                del f
        def enqueue_with_key(self, k, f, *params):
            def enqueue_job(k, f):
                self.lock.acquire()
                self.queue.append((k, f))
                self.cv.notify()
                self.lock.release()
            def mkjob(f, params):
                def g():
                    f(*params)
                return g
            enqueue_job(k, mkjob(f, params))
        def enqueue(self, f, *params):
            self.enqueue_with_key(None, f, *params)
        def cancel_all(self):
            newqueue = deque()
            self.lock.acquire()
            oldqueue = self.queue
            self.queue = newqueue
            self.lock.release()
            result = []
            while oldqueue:
                result.append(oldqueue.popleft())
            return map(lambda (k, f): k, result)
        def drain(self):
            self.cv.acquire()
            while self.queue:
                self.draincv.wait()
            self.cv.release()
        def stop(self):
            self.stopping = True
            self.lock.acquire()
            self.cv.notifyAll()
            self.lock.release()
    def __start_threads(self, n):
        class WorkerThread(threading.Thread):
            def __init__(self, f):
                self.fun = f
                threading.Thread.__init__(self)
            def run(self):
                self.fun()
        for i in range(n):
            w = WorkerThread(self.q.execute)
            self.threads.append(w)
            w.setDaemon(True)
            w.start()
    def __stop_threads(self):
        self.q.stop()
        for t in self.threads:
            t.join()
    def __init__(self, n):
        self.q = self.WorkQueue()
        self.enqueue = self.q.enqueue
        self.enqueue_with_key = self.q.enqueue_with_key
        self.drain = self.q.drain
        self.cancel_all = self.q.cancel_all
        self.threads = []
        try:
            self.__start_threads(n)
        except:
            try:
                self.__stop_threads()
            except:
                pass
            raise
    def __del__(self):
        try:
            self.__stop_threads()
        except:
            pass

# testcode
if __name__ == "__main__":
    w = Worker(16)
    for i in range(32):
        def f(i):
            if (i % 3) == 0:
                w.enqueue(f, i + 101)
            print i
        w.enqueue(f, i)
    w.drain()
