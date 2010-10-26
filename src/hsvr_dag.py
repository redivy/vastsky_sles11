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

__version__ = '$Id: hsvr_dag.py 252 2010-09-16 07:07:29Z yamamoto2 $'

import threading

import dag
import mynode
import symlinknode

from vas_conf import *

module_lock = threading.Lock()
modules = {}

def get_module_by_lvoltype(type):
    module_lock.acquire()
    try:
        if modules.has_key(type):
            module_lock.release()
            return modules[type]
        name = "lvnode_%u" % type
        m = __import__(name, globals(), locals(), [])
        try:
            m.module_init()
        except AttributeError:
            pass
        except:
            raise
        modules[type] = m
    except:
        module_lock.release()
        raise
    module_lock.release()
    return m

def create_dag_from_lvolstruct(lvolstruct, detach):
    def add_component(p, n, dag_only=False):
        if not dag_only:
            p.add_component(n)
        if detach:
            n.add_antecedent(p)
        else:
            p.add_antecedent(n)
    return create_dag_from_lvolstruct2(lvolstruct, add_component)

def create_dag_from_lvolstruct2(lvolstruct, add_component):
    def regnode(n):
        nodes.append(n)
    def mknode(l):
        def create_lvol_node():
            type = l['lvoltype']
            m = get_module_by_lvoltype(type)
            return m.create(l, regnode, add_component, components)
        components = []
        if l.has_key('components'): # XXX replaceMirrorDisk
            for cl in l['components']:
                components.append(mknode_with_labels(cl))
        n = create_lvol_node()
        regnode(n)
        for c in components:
            add_component(n, c)
        return n
    def mknode_with_labels(l):
        n = mknode(l)
        for label in l['labels']:
            s = symlinknode.SymLinkNode("%s/%s" % (VAS_DEVICE_DIR, label))
            # see the XXXkludge comment below.
            labels.append(s)
            s.add_component(n)
        return n
    labels = []
    nodes = []
    mknode_with_labels(lvolstruct)
    # XXXkludge; process labels after all other nodes.
    # in the case of detach failure with EBUSY, we don't want to remove
    # symlinks.
    noop = mynode.MyNode("noop_before_labels")
    for n in nodes:
        noop.add_antecedent(n)
    regnode(noop)
    for s in labels:
        s.add_antecedent(noop)
        regnode(s)
    return nodes
