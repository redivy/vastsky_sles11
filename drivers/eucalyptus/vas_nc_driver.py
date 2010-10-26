#!/usr/bin/env python

# $Id: vas_nc_driver.py 338 2010-10-22 03:22:20Z sugihara $

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


from vas_euca_driver import *

def attachVolume():

    hsvrid = get_my_hsvrid()
    if hsvrid == None:
        logger.error("abort. couldn't find head server id of the NC.")
        sys.exit(1)

    try:
        vas_send_request('attachLogicalVolume', {'lvolname': lvolname, 'hsvrid': hsvrid})
    except:
        logger.error('attachVolume failed')
        sys.exit(1)

def detachVolume():
    try:
        vas_send_request('detachLogicalVolume', {'lvolname': lvolname})
    except:
        logger.error('detachVolume failed')
        sys.exit(1)



if __name__ == '__main__':
    logger.debug('command:%s invoked.' % ' '.join(sys.argv))

    cmdname = os.path.basename(sys.argv[0])
    assert cmdname.startswith('vas_')

    assert len(sys.argv) == 3
    scName = sys.argv[1]
    volumeId = sys.argv[2]
    lvolname = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, volumeId) 

    euca_method = cmdname[4:]
    f = getattr(sys.modules['__main__'], euca_method)
    if f is None:
        logger.error('Aborted. Invalid method: ' + euca_method)
        sys.exit(1)

    sys.exit(f())

