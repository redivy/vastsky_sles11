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

__version__ = '$Id: ssvr_list.py 300 2010-10-07 03:59:43Z h-takaha $'

import sys
import getopt
import xmlrpclib
import socket
from vas_subr import *
from vas_const import ALLOC_PRIORITY_STR

def usage_listStorageServers(argv):
    print >> sys.stderr, 'usage: %s [-h|--help] [StorageServerID]' % (argv[0])

def getopt_listStorageServers(argv):

    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError:
        usage_listStorageServers(argv)
        sys.exit(2)

    obj = {}

    for o, _ in opts:
        if o in ("-h", "--help"):
            usage_listStorageServers(argv)
            sys.exit(2)

    if len(args) > 1:
        usage_listStorageServers(argv)
        sys.exit(2)

    try:
        if len(args) == 1:
            obj['ssvrid'] = get_targetid(args[0], 'ssvr-')

    except ValueError, inst:
        print >> sys.stderr, inst
        usage_listStorageServers(argv)
        sys.exit(2)

    return obj

def listStorageServers_print(array):
    column = "%-14s %-14s %-14s"
    capacity_total = 0
    available_total = 0
    for entry in array:
        capacity_total += entry['capacity']
        available_total += entry['available']
    if capacity_total:
        print column % ('total %d GB' % capacity_total, 'available %d GB' % available_total, 'use%% %d%%' % ((capacity_total - available_total) * 100/ capacity_total))
    else:
        print column % ('total %d GB' % capacity_total, 'available %d GB' % available_total, 'use% 0%')

    column = "%-14s %-9s %5s %10s %10s %5s"
    header = column % ('ssvrid', 'priority', 'resync', 'capacity', 'available', 'use%')
    print header
    for entry in array:
        if entry['capacity']:
            use = ((entry['capacity'] - entry['available']) * 100 / entry['capacity'])
        else:
            use = 0
        print column % ('ssvr-%08x' % entry['ssvrid'], ALLOC_PRIORITY_STR[entry['priority']], \
        '%d' % entry['resync'], '%3d GB' % entry['capacity'],  \
        '%d GB' % entry['available'], '%d%%' % use)
        for ip in entry['ip_data']:
            print "\tinet %s" % ip

def main():
    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    subargs = getopt_listStorageServers(sys.argv)
    subargs['ver'] = XMLRPC_VERSION

    try:
        res = send_request(host_storage_manager_list, port_storage_manager, "listStorageServers", subargs)
        listStorageServers_print(res)
    except xmlrpclib.Fault, inst:
        if inst.faultCode == 500:
            print >> sys.stderr, "Internal server error"
        else:
            print >> sys.stderr, os.strerror(inst.faultCode)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
