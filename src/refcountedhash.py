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

__version__ = '$Id: refcountedhash.py 252 2010-09-16 07:07:29Z yamamoto2 $'

# XXX is this necessary?  python objects themselves are ref-counted in
# a thread-safe manner?

import threading

class RefcountedHash(object):
    class Ref(object):
        def __init__(self, obj):
            self.count = 0
            self.obj = obj
    def __init__(self, ctr):
        self.lock = threading.Lock()
        self.hash = {}
        self.ctr = ctr
    def get(self, k):
        newref = None
        self.lock.acquire()
        try:
            ref = self.hash[k]
        except:
            self.lock.release()
            newref = self.Ref(self.ctr(k))
            self.lock.acquire()
            try:
                ref = self.hash[k]
            except:
                self.hash[k] = newref
                ref = newref
        ref.count += 1
        self.lock.release()
        del newref
        return ref.obj
    def put(self, k):
        self.lock.acquire()
        ref = self.hash[k]
        assert ref.count > 0
        ref.count -= 1
        if ref.count == 0:
            del self.hash[k]
        self.lock.release()

# testcode
if __name__ == "__main__":
    class A:
        def __init__(self, k):
            self.k = k
            print "ctr %s" % self
        def __del__(self):
            print "dtr %s" % self
        def __str__(self):
            return self.k
    h = RefcountedHash(A)
    def get(a):
        print "get %s" % a
        return h.get(a)
    def put(a):
        print "put %s" % a
        return h.put(a)
    o = get("foo")
    print o
    del o
    o = get("foo")
    print o
    del o
    put("foo")
    o = get("bar")
    print o
    del o
    put("bar")
    put("foo")
    o = get("foo")
    print o
    del o
    put("foo")
