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

__version__ = '$Id: lvnode_2.py 300 2010-10-07 03:59:43Z h-takaha $'

import os
import threading
import time
import lvnode
from vas_conf import *
from vas_const import MIRROR_STATUS
from vas_subr import blockdev_getsize, executecommand, getMirrorDevPath

# XXX workaround for the lack of locking in mdadm
mdadm_lock = threading.Lock()

def executemdadm(mdcommand):
    mdadm_lock.acquire()
    try:
        executecommand(mdcommand)
    except:
        mdadm_lock.release()
        raise
    mdadm_lock.release()

class MirrorNode(lvnode.LVNode):
    def __init__(self, lvolstruct):
        lvnode.LVNode.__init__(self, lvolstruct, "mirror")
    def _do(self):
        assert(len(self.components) > 0)
        path = getMirrorDevPath(self.lvolid)
        if not os.path.exists(path):
            do_assemble = False
            mirrordevs = ""
            for c in self.components:
                blockdev_getsize(c.path) # XXX is this necessary?
                if c.mirror_status == MIRROR_STATUS['ALLOCATED']:
                    mirrordevs += " " + c.path
                elif c.mirror_status == MIRROR_STATUS['INSYNC']:
                    mirrordevs += " " + c.path
                    do_assemble = True
            mdcommand = ""
            if do_assemble:
                mdcommand = "mdadm --assemble %s %s %s" % \
                    (path, MDADM_ASSEMBLE_OPTIONS, mirrordevs)
                executemdadm(mdcommand)
            else: # all extents are ALLOCATED. do --create
                mdcommand = \
                    "mdadm --create %s %s --assume-clean --raid-devices=%d %s" \
                        % (path, MDADM_CREATE_OPTIONS, len(self.components), \
                        mirrordevs)
                executemdadm(mdcommand)
            blockdev_getsize(path) # XXX is this necessary?
        self.path = path
    def do(self):
        try:
           self._do()
        except:
           # mdadm leaves the volume halfly baked on errors.
           # try to clean it up.
           try:
              self.undo()
           except:
              pass
           raise
    def undo(self):
        path = getMirrorDevPath(self.lvolid)
        assert(len(self.components) > 0)
        if os.path.exists(path):
            executecommand("mdadm -S %s" % path)
            executecommand("rm -f %s" % path)
        self.path = None

def create(l, regnode, add_component, components):
    return MirrorNode(l)
