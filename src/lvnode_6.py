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

__version__ = '$Id: lvnode_6.py 293 2010-10-04 06:47:48Z yamamoto2 $'

import os
import lvnode
import symlinknode
from vas_conf import *
from vas_subr import dmsetup_remove, execute_retry_not_path_exist, \
    getDmDevPath, getSnapshotOriginDevName, getSnapshotOriginOriginPath, gtos

class SnapshotOriginNode(lvnode.LVNode):
    def __init__(self, lvolstruct):
        lvnode.LVNode.__init__(self, lvolstruct, "snapshot-origin")
    def do(self):
        assert(len(self.components) > 0)
        name = getSnapshotOriginDevName(self.lvolid)
        path = getDmDevPath(name)
        if not os.path.exists(path):
            c = self.components[0]
            command = "echo 0 %s snapshot-origin %s | dmsetup create %s" % \
                (gtos(self.capacity), c.path, name)
            execute_retry_not_path_exist(command, path, DMSETUP_RETRY_TIMES)
        self.path = path
    def undo(self):
        assert(len(self.components) > 0)
        name = getSnapshotOriginDevName(self.lvolid)
        path = getDmDevPath(name)
        if os.path.exists(path):
            dmsetup_remove(name)
        self.path = None

def create(l, regnode, add_component, components):
    assert(len(components) == 1)
    origin = SnapshotOriginNode(l)
    c = components[0]
    # create a symlink so that SNAPSHOT module can find our underlying
    # device later.
    s = symlinknode.SymLinkNode(getSnapshotOriginOriginPath(origin.lvolid))
    regnode(s)
    add_component(s, c)
    return origin

def module_init():
    # XXX this should be done on startup of the machine, not hsvr_agent
    if os.path.exists(VAS_SNAPSHOT_DIR):
        for e in os.listdir(VAS_SNAPSHOT_DIR):
           os.remove("%s/%s" % (VAS_SNAPSHOT_DIR, e))
    else:
        os.makedirs(VAS_SNAPSHOT_DIR)
