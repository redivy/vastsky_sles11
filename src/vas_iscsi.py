

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

__version__ = '$Id: vas_iscsi.py 295 2010-10-05 00:00:31Z yamamoto2 $'

import re
import vas_subr
from vas_conf import *

class iScsiTarget:
    def __init__(self):
        self.type = "null"

    def gettype(self):
        return self.type

    def getTidTable(self):
        raise Exception, "invalid iscsi target"

    def newTarget(self, tid, iqn, pdskid):
        raise Exception, "invalid iscsi target"

    def newLogicalUnit(self, pdskid, tid, path):
        raise Exception, "invalid iscsi target"

    def delTarget(self, tid):
        raise Exception, "invalid iscsi target"

class TGT(iScsiTarget):
    def __init__(self):
        self.type = "tgt"

    def getTidTable(self):
        tid_re = re.compile("^Target")
        t = {}

        output = vas_subr.executecommand("tgtadm --lld iscsi --op show --mode=target")
        lines = output.splitlines()
        for line in lines:
            words = line.split(' ')
            if tid_re.match(words[0]):
                t[int(words[1].split(':')[0], 10)] = int(words[-1].split(':')[-1], 16)
        return t

    def newTarget(self, tid, iqn, pdskid):
        vas_subr.executecommand("tgtadm --lld iscsi --op new --mode=target --tid=%d --targetname %s:%08x" % (tid, iqn, pdskid))
        vas_subr.executecommand("tgtadm --lld iscsi --op bind --mode=target --tid=%d --initiator-address=ALL" % (tid))

    def newLogicalUnit(self, pdskid, tid, path):
        vas_subr.executecommand("tgtadm --lld iscsi --op new --mode=logicalunit --tid=%d --lun=1 --backing-store %s %s " % (tid, path, TGTADM_PARAMS_LUN))
        vas_subr.executecommand("tgtadm --lld iscsi --op update --mode=logicalunit --tid=%d --lun=1 --params scsi_id=%08x,scsi_sn=%08x" % (tid, pdskid, pdskid))

    def delTarget(self, tid):
        vas_subr.executecommand("igtadm --lld iscsi --op delete --mode=target --tid=%d" % (tid))


class IET(iScsiTarget):
    def __init__(self):
        self.type = "iet"

    def getTidTable(self):
        tid_re = re.compile("^tid:\d+$")
        t = {}

        f = open("/proc/net/iet/volume", 'r')
        while True:
            line = f.readline().rstrip()
            if not line:
                break
            tid_colon_num = line.split()[0]
            if tid_re.match(tid_colon_num):
                t[int(tid_colon_num.split(':')[1], 10)] = int(line.split(':')[-1], 16)
        return t

    def newTarget(self, tid, iqn, pdskid):
        vas_subr.executecommand("ietadm --op new --tid=%d --params Name=%s:%08x" % (tid, iqn, pdskid))
        vas_subr.executecommand("ietadm --op update --tid=%d --params=%s" % (tid, IETADM_PARAMS_TID))

    def newLogicalUnit(self, pdskid, tid, path):
        vas_subr.executecommand("ietadm --op new --tid=%d --lun=1 --params Path=%s,ScsiId=%08x,ScsiSN=%08x,%s" % (tid, path, pdskid, pdskid, IETADM_PARAMS_LUN))

    def delTarget(self, tid):
        vas_subr.executecommand("ietadm --op delete --tid=%d" % (tid))

def select_iScsiTarget(type):
    type = type.upper()
    if type == "IET":
        o = IET()
    elif type == "TGT":
        o = TGT()
    else:
        o = iScsiTarget()
    return o

