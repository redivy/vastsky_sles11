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

__version__ = '$Id: snap_create.py 279 2010-09-29 05:48:11Z yamamoto2 $'

import sys
import getopt
import xmlrpclib
import socket
from vas_subr import *

def usage_createSnapshot(argv):
    print >> sys.stderr, 'usage: %s [-h|--help] \
        {LogicalVolumeID|LogicalVolumeName} [SnapshotName]' % (argv[0])

def getopt_createSnapshot(argv):
    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError:
        usage_createSnapshot(argv)
        sys.exit(2)

    obj = {}

    try:
        for o, a in opts:
            if o in ("-h"):
                usage_createSnapshot(argv)
                sys.exit(2)

        if len(args) != 2:
            usage_createSnapshot(argv)
            sys.exit(2)

        try:
            obj['origin_lvolid'] = get_targetid(args[0], 'lvol-')
        except:
            obj['origin_lvolname'] = args[0]
        obj['snapshot_lvolname'] = args[1]
    except ValueError:
        usage_createSnapshot(argv)
        sys.exit(2)

    return obj

def main():
    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    subargs = getopt_createSnapshot(sys.argv)
    subargs['ver'] = XMLRPC_VERSION

    try:
        res = send_request(host_storage_manager_list, port_storage_manager, \
            "createSnapshot", subargs)
        print "lvol-%08x" % res
    except xmlrpclib.Fault, inst:
        if inst.faultCode == 500:
            print >> sys.stderr, "Internal server error"
        else:
            print >> sys.stderr, os.strerror(inst.faultCode)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
