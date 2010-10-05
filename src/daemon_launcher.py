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

# start a daemon and keep it running by restarting.

__version__ = '$Id: daemon_launcher.py 114 2010-07-23 04:54:52Z yamamoto2 $'

import os
import sys
import socket
import getopt
import re
import signal
from vas_conf import *
from vas_subr import daemonize

def usage():
    print """
usage: %s option daemon_path pid_file
options:
   -a, --inaddr_any         invoke single <daemon_path> with -h 0.0.0.0 option
   -n, --nproc number       invoke number of <daemon_path> without any options
   """ % (sys.argv[0])

def sighandler(signum, frame):
    for index in daemons:
        if daemons[index]:
            os.kill(int(daemons[index]), signal.SIGTERM)
            daemons[index] = 0
    sys.exit(0)

def system_down():
    executecommand(SHUTDOWN_CMD)

daemons = {}

def main():
    try:
        opts, exec_args = getopt.getopt(sys.argv[1:], "an:", ["inaddr_any", "nproc="])
        if len(exec_args) < 2:
            usage()
            sys.exit(2)    
    except getopt.GetoptError:
        print "GetoptError"
        usage()
        sys.exit(2)

    ip_addr_re = re.compile("^\d+\.\d+\.\d+\.\d+$")
    ipaddrlist = ()
    nproc = 1

    try:
        aflag = 0
        nflag = 0
        for o, a in opts:
            if o in ("-a", "--inaddr_any"):
                ipaddrlist = ['0.0.0.0']
                aflag = 1

            elif o in ("-n", "--nproc"):
                if not a.isdigit():
                    raise Exception, 'invalid number: %s' % (a)
                nproc = int(a)
                if nproc < 1:
                    raise Exception, 'number(%s) too small. must be larger than 0.' % (a)
                nflag = 1

        if aflag + nflag > 1:
            raise Exception, "can not specify both `-a' and '-n' option"
            
        if not os.path.exists(exec_args[0]):
            raise Exception, 'daemon(%s) not found.' % exec_args[0]

    except Exception, inst:
        print >> sys.stderr, inst
        usage()
        sys.exit(2)

    if len(ipaddrlist) == 0:
        # invoke nproc daemons 
        for index in range(nproc):
            daemons[index] = 0
    else:
        # invoke daemons for each ipaddrs
        for index in ipaddrlist:
            daemons[index] = 0

    pid = daemonize()
    f = open(exec_args[1], "w")
    f.writelines("%d\n" % pid)
    f.close()
    exec_args.pop(1)

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    for index in daemons:
        if daemons[index] == 0:
            daemons[index] = os.fork()
            if daemons[index] == 0:
                if len(ipaddrlist):
                    exec_args.append('-h %s' % (index))
                os.execv(exec_args[0], exec_args)
                break

    count = SHUTDOWN_GRACE_COUNT
    while True:
        try:
            pid,status = os.waitpid(0,0)
            count -= 1
            if count <= 0:
                logger.error("%s: shutdown" % exec_args[0])
                system_down()
                sys.exit(1)
            for index in daemons:
                if daemons[index] == pid:
                    daemons[index] = os.fork()
                    if daemons[index] == 0:
                        if len(ipaddrlist):
                            exec_args.append('-h %s' % (index))
                        os.execv(exec_args[0], exec_args)
                        break
        except:
            sys.exit(1)

if __name__ == "__main__":
    main()
