
# $Id: vas_euca_driver.py 338 2010-10-22 03:22:20Z sugihara $

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


import commands
import sys
import xmlrpclib
import os.path

sys.path = sys.path + ['/opt/vas/lib', '/opt/vastorage/lib']
from vas_conf import logger
import vas_subr

LVOLNAME_PREFIX='euca'

def vas_send_request(method, args):
    try:
        args['ver'] = vas_subr.XMLRPC_VERSION
        res = vas_subr.send_request(vas_subr.host_storage_manager_list, \
            vas_subr.port_storage_manager, method, args)
    except xmlrpclib.Fault, inst:
        logger.error("euca_driver: %s fault %d" % (method, inst.faultCode))
        raise
    except:
        logger.error("euca_driver: %s exception" % method)
        raise
    return res


def get_my_hsvrid():
    s, ip_o = commands.getstatusoutput('/sbin/ip -o addr')
    if s !=0:
        logger.error("/sbin/ip failed(%s)." % s)
        sys.exit(1)
    try:
        res = vas_send_request('listHeadServers', {})
    except:
        sys.exit(1)

    for hs in res:
        for ip in hs['ip_data']:
            for l in ip_o.splitlines():
                if l.find(ip) >= 0:
                    return hs['hsvrid'] 
    return None

