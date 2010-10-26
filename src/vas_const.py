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

__version__ = '$Id: vas_const.py 319 2010-10-20 05:54:09Z yamamoto2 $'

from vas_subr import __reverse_dict

# constants larger than 100 are reserved by VA Linux Systems Japan
ALLOC_PRIORITY = {'HIGH':1, 'LOW':2, 'EVACUATE':3, 'OFFLINE':4, 'FAULTY':5, 'HALT':6}
ALLOC_PRIORITY_STR = __reverse_dict(ALLOC_PRIORITY)
EXT_STATUS = {'BUSY':1, 'FREE':2, 'EVACUATE':3, 'OFFLINE':4, 'SUPER':5, 'FAULTY':6}
# ATTACH_STATUS: 0 and 4 are reserved by VA Linux Systems Japan
# UNBOUND is for in-core use.  it never appears in the db.
ATTACH_STATUS = {'UNBOUND': 0, 'BOUND':1, 'ERROR': 2}
ATTACH_EVENT = {'BINDING':1, 'UNBINDING':2}
MIRROR_STATUS = {'ALLOCATED': 1, 'INSYNC': 2, 'SPARE': 3, 'FAULTY': 4, 'NEEDSHRED': 5}
MIRROR_STATUS_STR = __reverse_dict(MIRROR_STATUS)
# LVOLTYPE: 100 and higher LVOLTYPEs are reserved by VA Linux Systems Japan
LVOLTYPE = {'LVOL': 0, 'LINEAR':1, 'MIRROR':2, 'DEXT':3, 'SNAPSHOT-ORIGIN':6, \
    'SNAPSHOT':7}
TARGET = {'HSVR':1, 'SSVR':2, 'PDSK': 3, 'LVOL':4}
TARGET_PREFIX = ['none-', 'hsvr-', 'ssvr-', 'pdsk-', 'lvol-']
IDTYPE = {'hsvr': 1, 'lvol': 2, 'pdsk': 3, 'ssvr': 4, 'event': 5}

