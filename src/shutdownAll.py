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

__version__ = '$Id: shutdownAll.py 279 2010-09-29 05:48:11Z yamamoto2 $'

import sys
import getopt
import xmlrpclib
import socket
from vas_subr import *

def usage_shutdownAll(argv):
    print >> sys.stderr, 'usage: %s %s [-h|--help]' % (argv[0])

def getopt_shutdownAll(argv):

    try:
        opts, args = getopt.getopt(argv[2:], "h", ["help"])
    except getopt.GetoptError:
        usage_shutdownAll(argv)
        sys.exit(2)

    obj = {}

    for o, a in opts:
        if o in ("-h", "--help"):
            usage_shutdownAll(argv)
            sys.exit(2)

    if len(args) != 0:
        usage_shutdownAll(argv)
        sys.exit(2)

    return obj

def main():
    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    subargs = getopt_shutdownAll(sys.argv)
    subargs['ver'] = XMLRPC_VERSION

    try:
        send_request(host_storage_manager_list, port_storage_manager, "shutdownAll", subargs)
        sys.exit(0)

    except xmlrpclib.Fault, inst:
        sys.exit(inst.faultCode)

if __name__ == "__main__":
    main()
