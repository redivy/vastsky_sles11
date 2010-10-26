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

__version__ = '$Id: dag.py 291 2010-10-04 04:58:20Z yamamoto2 $'

import sys
import threading
import traceback
import worker

class DAGNode:
    def __init__(self):
        self.antecedents = []
        self.succedents = []
    def add_antecedent(self, a):
        self.antecedents.append(a)
        a.succedents.append(self)

def dag_execute(nodes, f, u, w, logger=None):
    def noop(n, e, u):
        pass
    def antecedents(n):
        if undoing:
            return n.succedents
        else:
            return n.antecedents
    def succedents(n):
        if undoing:
            return n.antecedents
        else:
            return n.succedents
    def run_node(n, f):
        logger(n, "run", undoing)
        try:
            f(n)
            fail = False
        except AssertionError:
            ei = sys.exc_info()
            t, v, tr = ei
            logger(n, "assertfail %s" % \
                traceback.format_exception(t, v, tr),
                undoing)
            raise # XXX
        except:
            fail = True
            ei = sys.exc_info()
            if undoing:
                t, v, tr = ei
                logger(n, "exception %s" % \
                    traceback.format_exception(t, v, tr),
                    undoing)
        lock.acquire()
        assert n in inflight
        assert not n in done
        assert not n in todo
        inflight.remove(n)
        if fail:
            logger(n, "fail", undoing)
            failed.append((n, ei))
            del ei
            cancelled = w.cancel_all()
            map(lambda x: logger(x, "cancel", undoing), cancelled)
            map(inflight.remove, cancelled)
            map(todo.append, cancelled)
        else:
            logger(n, "done", undoing)
            done.append(n)
            if not failed:
                try_schedule(filter(lambda x: x in todo, succedents(n)))
        if not inflight:
            cv.notify()
        lock.release()
    def can_run_p(n):
        assert n in todo
        for x in antecedents(n):
            if not x in done:
                assert n in inflight or n in todo
                return False
        return True
    def try_schedule(xs):
        for n in filter(can_run_p, xs):
            todo.remove(n)
            inflight.append(n)
            logger(n, "schedule", undoing)
            w.enqueue_with_key(n, run_node, n, f)
    def run_and_wait():
        try_schedule(todo)
        while inflight:
            cv.wait()
    if not logger:
        logger = noop
    lock = threading.Lock()
    cv = threading.Condition(lock)
    undoing = False
    todo = nodes[:]
    inflight = []
    failed = []
    done = []
    lock.acquire()
    assert len(todo) + len(done) + len(failed) == len(nodes)
    run_and_wait()
    assert len(todo) + len(done) + len(failed) == len(nodes)
    if failed:
        result = failed
        undoing = True
        t = todo + map(lambda (x,y): x, failed)
        todo = done
        done = t
        failed = []
        assert not inflight
        f = u
        run_and_wait()
        assert (not todo)
        assert (not failed)
    else:
        result = []
    lock.release()
    return result

def dag_print(nodes):
    def f(n):
        print "%s depends on:" % n
        for i in n.antecedents:
            print "\t%s" % i
    dag_execute(nodes, f, None, worker.Worker(1))

# testcode
if __name__ == "__main__":
    import time
    import traceback
    import sys
    import random
    class MyNode(DAGNode):
        def __init__(self, label):
            self.label = label
            self.result = None
            DAGNode.__init__(self)
        def __str__(self):
            return "%s" % self.label
        def __repr__(self):
            return self.__str__()
    a = MyNode("A")
    b = MyNode("B")
    c = MyNode("C")
    d = MyNode("D")
    e = MyNode("E")
    f = MyNode("F")
    g = MyNode("G")
    h = MyNode("H")
    a.add_antecedent(b)
    a.add_antecedent(c)
    b.add_antecedent(f)
    c.add_antecedent(d)
    d.add_antecedent(e)
    e.add_antecedent(f)
    e.add_antecedent(g)
    e.add_antecedent(h)
    nodes = [a, b, c, d, e, f, g, h]
    nodestofail = []
    for x in range(100):
        n = MyNode("n-%u" % x)
        for x in nodes:
            if random.randint(0, 16) == 0:
                x.add_antecedent(n)
        nodes.append(n)
    def f(n):
        sys.stdout.write("%s start\n" % n)
        sys.stdout.flush()
        assert not n.result
        for x in n.antecedents:
            assert x.result
        for x in n.succedents:
            assert not x.result
#       time.sleep(1)
        if n in nodestofail:
            def myraise():
                raise "bar"
#           time.sleep(1)
            sys.stdout.write("%s fail\n" % n)
            sys.stdout.flush()
            if random.randint(0, 1) == 0:
                myraise()
            raise "foo"
        sys.stdout.write("%s end\n" % n)
        sys.stdout.flush()
        n.result = True
    def u(n):
        sys.stdout.write("%s undo start\n" % n)
        sys.stdout.flush()
        assert n.result
        for x in n.antecedents:
            assert x.result
        for x in n.succedents:
            assert not x.result
#       time.sleep(1)
        n.result = None
        sys.stdout.write("%s undo end\n" % n)
        sys.stdout.flush()
    dag_print(nodes)
    w = worker.Worker(16)
    def myprint(n, x, u):
        if u:
            sys.stdout.write("%s %s (dag undo)\n" % (n, x))
        else:
            sys.stdout.write("%s %s (dag)\n" % (n, x))
        sys.stdout.flush()
    xs = dag_execute(nodes, f, u, w, myprint)
    assert not xs
    for n in nodes:
        assert n.result
        n.result = None
    nodestofail = []
    for x in range(16):
        nodestofail.append(random.choice(nodes))
    xs = dag_execute(nodes, f, u, w, myprint)
    assert xs
    del w
    for x, ei in xs:
        print "%s failed" % x
        t, v, tr = ei
        print "======================"
        traceback.print_exception(t, v, tr)
        print "======================"
