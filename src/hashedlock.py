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

__version__ = '$Id: hashedlock.py 252 2010-09-16 07:07:29Z yamamoto2 $'

import threading
import refcountedhash

class HashedLock(object):
    def __init__(self):
        def ctr(k):
            return threading.Lock()
        self.hash = refcountedhash.RefcountedHash(ctr)
    def acquire(self, keys):
        skeys = sorted(keys)
        ukeys = []
        for x in skeys:
            if not x in ukeys:
                ukeys.append(x)
        locks = map(self.hash.get, ukeys)
        map(lambda x: x.acquire(), locks)
        return (locks, ukeys)
    def release(self, h):
        locks, skeys = h
        map(lambda x: x.release(), locks)
        map(self.hash.put, skeys)

# testcode
if __name__ == "__main__":
    import random
    import time
    hlock = HashedLock()
    h = hlock.acquire(["foo", "bar"])
    print h
    hlock.release(h)
    h = hlock.acquire(["foo", "foo"])
    print h
    del h
    lock = threading.Lock()
    hash = {}
    class MyThread(threading.Thread):
        def run(self):
            for x in range(32):
                rs = []
                for x in range(16):
                    rs.append(random.randint(0, 100))
                print "%s start" % rs
                h = hlock.acquire(rs)
                lock.acquire()
                for r in rs:
                    assert not hash.has_key(r) or hash[r] == self
                    hash[r] = self
                lock.release()
                time.sleep(0.1)
                lock.acquire()
                for r in rs:
                    if hash.has_key(r):
                        assert hash[r] == self
                        del hash[r]
                lock.release()
                hlock.release(h)
                print "%s end" % rs
            return 0
    threads = []
    for x in range(100):
        t = MyThread()
        t.start()
        threads.append(t)
    for t in threads:
        r = t.join()
        assert r == 0
