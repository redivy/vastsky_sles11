#!/usr/bin/env python

# $Id: vas_sc_driver.py 341 2010-10-22 03:58:40Z sugihara $

#
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

#
#
#  This provides vas driver for Storage Controller of Eucalyptus

from vas_euca_driver import *

def createSnapshot(args):
    scName = args[0]
    volumeId = args[1]
    snapshotId = args[2]

    origin_lvolname = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, volumeId)
    try:
        res = vas_send_request('listLogicalVolumes', {'lvolname': origin_lvolname})
    except:
        logger.error("euca_driver: couldn't get volume: %s" % origin_lvolname)
        sys.exit(1)

    assert len(res) == 1

    snapname = "%s-%s-%s--of--%s" % (LVOLNAME_PREFIX, scName, snapshotId, origin_lvolname)
    try:
        res = vas_send_request('createSnapshot', {'origin_lvolname': origin_lvolname, 'snapshot_lvolname': snapname}) 
    except:
        logger.error('failed to create snapshot of vas volume: %s, %s' % (origin_lvolname, snapname))
        sys.exit(1)
    
def deleteSnapshot(args):
    scName = args[0]
    snapshotId = args[1]

    snap_lvolname_prefix = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, snapshotId)

    try:
        res = vas_send_request('listLogicalVolumes', {})
    except:
        logger.error("euca_driver: listLogicalVolumes failed" )
        sys.exit(1)

    if res is None:
        logger.error('Aborted. no volume found in the system')
        sys.exit(1)

    for lvol in res:
        if lvol['lvolname'].startswith(snap_lvolname_prefix):
            if lvol['hsvrid'] != 0:
                logger.error('euca_driver: Aborted. Cannot delete the snapshot because specified volume %s is attached.' % lvol['lvolname'] )
                sys.exit(1)
            snap_lvolname = lvol['lvolname']

    try:
        res = vas_send_request('deleteLogicalVolume', {'lvolname': snap_lvolname})
    except:
        logger.error('euca_driver: Aborted. deleteLogicalVolume Failed.')
        sys.exit(1)
     

def createVolume(args):
    if len(args) == 3: # corresponds VASManager.createVolume(String volumeId, int size) 
        scName = args[0]  # StorageController name: presumably identical to zone name.
        volumeId = args[1]
        size = int(args[2])  #in GB

        lvolname = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, volumeId)
        try:
            vas_send_request('createLogicalVolume', {'lvolname': lvolname, 'capacity': size})
        except:
            logger.error('Abort. createLogicalVolume failed.')
            sys.exit(1)

    elif len(args) == 4: # corresponds VASManager.createVolume(String volumeId, String snapshotId, int size) 
        scName = args[0]  # StorageController name: presumably identical to zone name.
        volumeId = args[1]
        snapshotId = args[2]
        size = int(args[3])  #in GB


        my_hsvrid = get_my_hsvrid()
        if my_hsvrid == None:
            logger.error("abort. couldn't find head server id of me.")
            sys.exit(1)


        origin_lvolname = None
        try:
            res = vas_send_request('listLogicalVolumes', {})
        except:
            logger.error("euca_driver: listLogicalVolumes failed" )
            sys.exit(1)
        snap_lvolname_prefix =  '%s-%s-%s' % (LVOLNAME_PREFIX, scName, snapshotId)
        for lvol in res:
            if lvol['lvolname'].startswith(snap_lvolname_prefix):
                origin_lvolname = lvol['lvolname'][lvol['lvolname'].find('%s-%s-vol' % (LVOLNAME_PREFIX, scName)):]

        if origin_lvolname is None:
            logger.error('euca_driver: Abort. cannot find snapshot from which to create a volume')
            sys.exit(1)

        snap_src_name = "%s-%s-%s--of--%s" % (LVOLNAME_PREFIX, scName, snapshotId, origin_lvolname)

        # now try to get info of the original volume
        try:
            res = vas_send_request('listLogicalVolumes', {'lvolname': origin_lvolname})
        except:
            logger.error('listLogicalVolume %s failed.' % origin_lvolname)
            sys.exit(1)
       
        assert len(res) == 1  

        lvol = res[0]
        size_in_gb = lvol['capacity']
        attached_hsvrid =  lvol['hsvrid']
 
        origin_unattached = False
        if attached_hsvrid == 0:
            origin_unattached = True

#        print lvol 
#        print origin_unattached
  
        if origin_unattached is True or attached_hsvrid == my_hsvrid: # 0 emans not attached
            pass
        else: # currently the volume must not be attached to any node other than me
            logger.error('abort. the volume is attached to a different node than me.')
            sys.exit(1)


        if origin_unattached:  # if not attached, do attach so that its data can be read to create a copy volume
            try: 
                vas_send_request('attachLogicalVolume', {'lvolname': origin_lvolname, 'hsvrid': my_hsvrid})
            except:
                logger.error('abort.failed to attach ')
                sys.exit(1)
    
       # create a vas volume for the eucalyptus snapshot and attach it, then copy from the vas snapshot
        snap_lvolname = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, volumeId)
        try:
            res = vas_send_request('createLogicalVolume', {'lvolname': snap_lvolname, 'capacity': size_in_gb})
            res = vas_send_request('attachLogicalVolume', {'lvolname': snap_lvolname, 'hsvrid': my_hsvrid})
            s, o = commands.getstatusoutput('dd if=/dev/vas/%s of=/dev/vas/%s bs=1M' % (snap_src_name, snap_lvolname))
            if s !=0:
                logger.error('dd failed: %s, %s' % (s, o))
                sys.exit(1)
        except:
            logger.error('euca_driver: aborted.')
            sys.exit(1)
    
       # cleanup
        try: 
            vas_send_request('detachLogicalVolume', {'lvolname': snap_lvolname})
            if origin_unattached is True:  # cleanup: detach the volume
                vas_send_request('detachLogicalVolume', {'lvolname': snap_src_name})
        except:
            logger.error('abort.failed to cleanup.' )
            print sys.exc_info()[0]
            sys.exit(1)
    

def deleteVolume(args):
    scName = args[0]  # StorageController name: presumably identical to zone name.
    volumeId = args[1]

    lvolname = "%s-%s-%s" % (LVOLNAME_PREFIX, scName, volumeId)

    try:
        vas_send_request('deleteLogicalVolume', {'lvolname': lvolname})
    except:
        logger.error('Abort. deleteLogicalVolume failed.')
        sys.exit(1)

if __name__ == '__main__':
    logger.debug('command:%s invoked.' % ' '.join(sys.argv))

    cmdname = os.path.basename(sys.argv[0])
    assert cmdname.startswith('vas_')
    euca_method = cmdname[4:]

    f = getattr(sys.modules['__main__'], euca_method)
    if f is None:
        logger.error('Aborted. Invalid method: ' + euca_method)
        sys.exit(1)
    sys.exit(f(sys.argv[1:]))
    
