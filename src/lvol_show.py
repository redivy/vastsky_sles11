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

__version__ = '$Id: lvol_show.py 24 2010-07-05 02:58:29Z yamamoto2 $'

import sys
import getopt
import xmlrpclib
import socket
import string
from vas_subr import *
from vas_db import LVOLTYPE, MIRROR_STATUS_STR

def usage_showLogicalVolume(argv):
    print >> sys.stderr, 'usage: %s [-h|--help] {LogicalVolumeID|LogicalVolumeName}' % (argv[0])

def getopt_showLogicalVolume(argv):

    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError:
        usage_showLogicalVolume(argv)
        sys.exit(2)

    obj = {}

    for o, _ in opts:
        if o in ("-h", "--help"):
            usage_showLogicalVolume(argv)
            sys.exit(2)

    if len(args) != 1:
        usage_showLogicalVolume(argv)
        sys.exit(2)

    try:
        try:
            obj['lvolid'] = get_targetid(args[0], 'lvol-') 
        except:
            obj['lvolname'] = args[0]

    except ValueError, inst:
        print >> sys.stderr, inst
        usage_showLogicalVolume(argv)
        sys.exit(2)

    return obj

def showLogicalVolume_print(lvolstruct):

    column = "%-14s %8s %8s | %-14s %-14s %8s %-14s %-5s"
    def __show_components_dext(lvolstructs):
        for lvolstruct in lvolstructs:
            if lvolstruct['lvoltype'] == LVOLTYPE['DEXT']:

                lvolspec = lvolstruct['lvolspec']
                print column % ('', '', '', 'dext-%08x' % lvolstruct['lvolid'],'pdsk-%08x' % lvolspec['pdskid'],'%d GB' % lvolspec['offset'],'ssvr-%08x' % lvolspec['ssvrid'], MIRROR_STATUS_STR[lvolspec['status']])

    def __show_components_mirror(lvolstructs):
        print column % ('mirror', 'offset', 'capacity', 'disk-extent', 'physical-disk', 'offset', 'server', 'sync')
        offset = 0
        for lvolstruct in lvolstructs:
            if lvolstruct['lvoltype'] == LVOLTYPE['MIRROR']:

                lvolspec = lvolstruct['lvolspec']
                print column % ('mirr-%08x' % lvolstruct['lvolid'], '%d GB' % offset, '%d GB' % lvolstruct['capacity'], '','','','','')
                offset += lvolstruct['capacity']

                __show_components_dext(lvolstruct['components'])
            else:
                __show_components_dext(lvolstructs)

    def __show_component_linear(lvolstruct):
        if lvolstruct['lvoltype'] == LVOLTYPE['LINEAR']:

            lvolspec = lvolstruct['lvolspec']
	    capacity_str = '%d GB' % lvolstruct['capacity']
                
            print "lvolid: lvol-%08x lvol_name: %s capacity: %s redundancy: %d" % (lvolstruct['lvolid'], lvolspec['lvolname'], capacity_str, len(lvolstruct['components'][0]['components']))

            __show_components_mirror(lvolstruct['components'])
        else:
            __show_components_mirror([lvolstruct])

    return __show_component_linear(lvolstruct)

def main():
    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    subargs = getopt_showLogicalVolume(sys.argv)
    subargs['ver'] = XMLRPC_VERSION

    try:
        res = send_request(host_storage_manager_list, port_storage_manager, "showLogicalVolume", subargs)
        showLogicalVolume_print(res)
    except xmlrpclib.Fault, inst:
        if inst.faultCode == 500:
            print >> sys.stderr, "Internal server error"
        else:
            print >> sys.stderr, os.strerror(inst.faultCode)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
