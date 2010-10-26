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

__version__ = '$Id: lvnode_7.py 319 2010-10-20 05:54:09Z yamamoto2 $'

import os
import lvnode
import mynode
from vas_conf import *
from vas_subr import blockdev_getsize, dmsetup_remove, \
    execute_retry_not_path_exist, executecommand, getDmDevPath, \
    getSnapshotOriginDevName, getSnapshotOriginOriginPath, gtos

def getSnapshotDevName(lvolid):
    return "snapshot-%08x" % lvolid

def dm_suspend(lvolid):
    name = getSnapshotOriginDevName(lvolid)
    path = getDmDevPath(name)
    if os.path.exists(path):
        command = "dmsetup suspend %s" % name
        executecommand(command)

def dm_resume(lvolid):
    name = getSnapshotOriginDevName(lvolid)
    path = getDmDevPath(name)
    if os.path.exists(path):
        command = "dmsetup resume %s" % name
        executecommand(command)

class SuspendNode(mynode.MyNode):
    def __init__(self, lvolid):
        self.lvolid = lvolid
        mynode.MyNode.__init__(self, "suspend %08x" % lvolid)
    def do(self):
        dm_suspend(self.lvolid)
    def undo(self):
        dm_resume(self.lvolid)

class ResumeNode(mynode.MyNode):
    def __init__(self, lvolid):
        self.lvolid = lvolid
        mynode.MyNode.__init__(self, "resume %08x" % lvolid)
    def do(self):
        dm_resume(self.lvolid)
    def undo(self):
        dm_suspend(self.lvolid)

class SnapshotNode(lvnode.LVNode):
    def __init__(self, lvolstruct):
        self.snapshot_origin_lvolid = lvolstruct['lvolspec']['origin_lvolid']
        lvnode.LVNode.__init__(self, lvolstruct, "snapshot")
    def do(self):
        assert(len(self.components) > 0)
        name = getSnapshotDevName(self.lvolid)
        path = getDmDevPath(name)
        if not os.path.exists(path):
            origin_path = getSnapshotOriginOriginPath( \
                self.snapshot_origin_lvolid)
            origin_size = blockdev_getsize(origin_path)
            c = self.components[0]
            command = \
                "echo 0 %s snapshot %s %s P %u | dmsetup create --readonly %s" \
                % (origin_size, origin_path, c.path, SNAPSHOT_CHUNK_SIZE, name)
            execute_retry_not_path_exist(command, path, DMSETUP_RETRY_TIMES)
        self.path = path
    def undo(self):
        assert(len(self.components) > 0)
        name = getSnapshotDevName(self.lvolid)
        path = getDmDevPath(name)
        if os.path.exists(path):
            dmsetup_remove(name)
        self.path = None

def create(l, regnode, add_component, components):
    snapshot = SnapshotNode(l)
    suspend = SuspendNode(snapshot.snapshot_origin_lvolid)
    resume = ResumeNode(snapshot.snapshot_origin_lvolid)
    regnode(suspend)
    regnode(resume)
    add_component(snapshot, suspend, True)
    add_component(resume, snapshot, True)
    return snapshot
