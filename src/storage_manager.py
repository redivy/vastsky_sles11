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

__version__ = '$Id: storage_manager.py 321 2010-10-20 05:59:06Z yamamoto2 $'

import sys
import getopt
import SimpleXMLRPCServer
import xmlrpclib
import socket
import vas_db
import signal
import os
import traceback
import time
import errno
import re
import threading
from SocketServer import ThreadingMixIn
from types import *
from vas_conf import *
from vas_subr import lineno, gtos, stovb, vbtos, executecommand, get_lvolstruct_of_lvoltype, \
   getRoundUpCapacity, check_lvolname, get_iscsi_path, mand_keys, start_worker
from lv_dbnode import *
from event import EVENT_STATUS
from lv_dbnode import *

def lvol_find_assoc_root(db, lvolid):
    assoc = db.assoc.get_rows('assoc_lvolid', lvolid)
    if not assoc:
        return lvolid
    else:
        assert(len(assoc) == 1)
        return lvol_find_assoc_root(db, assoc[0]['lvolid'])

def lvol_get_attach(db, lvolid):
    root_lvolid = lvol_find_assoc_root(db, lvolid)
    attach = db.attach.get_row(root_lvolid)
    return attach

def lvol_db_assoc(db, lvolid, assoc_lvolid, type):
    attach = lvol_get_attach(db, lvolid)
    if attach and attach['status'] != vas_db.ATTACH_STATUS['BOUND']:
        raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
    db.assoc.put_row((lvolid, assoc_lvolid, type))

class SMException:
    pass

class SMExceptionSocketTimeout(SMException):
    pass

class SMExceptionBothLinkDown(SMException):
    pass

class StorageManager:
    def __init__(self):
        try:
            self.connect_db()
            self.__salvage_resync()
            self.__salvage_shred()
            self.resync_cv = threading.Condition(main_lock)
            start_worker(self.__resync_worker)
            self.shred_cv = threading.Condition(main_lock)
            start_worker(self.__shred_worker)
            self.__salvage_attach()
        except:
            logger.error(traceback.format_exc())
            sys.exit(1)

    def __salvage_attach(self):
        logger.debug("__salvage_attach start")
        self.attach_table = {}
        main_lock.acquire() 
        try:
            attach_rows = self.db.attach.get_rows('eventid <> 0', '*')
            for attach in attach_rows:
                eventid = attach['eventid']
                type = attach['event_type']
                lvolid = attach['lvolid']
                snapshot_lvolid = attach['assoc_lvolid']
                ip_addrs = self.db.ipdata.hsvr_ipdata(attach['hsvrid'])
                if snapshot_lvolid:
                    assert(attach['lvolid'] == \
                        lvol_find_assoc_root(self.db, snapshot_lvolid))
                    lvolstruct = get_lvolstruct(self.db, snapshot_lvolid)
                else:
                    lvolstruct = get_lvolstruct(self.db, lvolid)
                if type == vas_db.ATTACH_EVENT['BINDING']:
                    start_worker(self.__lvolops_worker, ip_addrs, eventid, \
                        "attachLogicalVolume", lvolstruct, self.__lvol_attach_done)
                elif type == vas_db.ATTACH_EVENT['UNBINDING']:
                    start_worker(self.__lvolops_worker, ip_addrs, eventid, \
                        "detachLogicalVolume", lvolstruct, self.__lvol_detach_done)
        except Exception:
            logger.error(traceback.format_exc())
        main_lock.release()

    def _dispatch(self, method, params):
        global main_lock
        logger.info("DISPATCH %s called. %s" % (method, params))
        try:
            f = getattr(self, method)
        except AttributeError:
            raise Exception('method "%s" is not supported' % method)
        except Exception, inst:
            logger.info("DISPATCH getattr(%s) EXCEPTION %s" % \
            (method, inst))
            raise
        main_lock.acquire()
        try:
            if not params[0].has_key('ver') or params[0]['ver'] != XMLRPC_VERSION:
                raise xmlrpclib.Fault(errno.EPROTO, 'EPROTO')
            ret = f(*params)
            main_lock.release()
        except Exception, inst:
            main_lock.release()
            logger.info("DISPATCH %s EXCEPTION %s" % (method, inst))
            raise
        logger.info("DISPATCH %s returned %s" % (method, ret))
        return ret
        
    def connect_db(self):
        if not os.path.exists(DB_COMPONENTS):
            raise Exception, "%s not exists." % (DB_COMPONENTS)
        self.db = vas_db.Db_components(DB_COMPONENTS)
        self.db.connect()

    def disconnect_db(self):
        self.db.disconnect()

    def __count_valid_copy(self, dextid):
        lvolmap = self.db.lvolmap.get_row(dextid)
        dexts = self.db.lvolmap.get_rows('superlvolid', lvolmap['superlvolid'])
        count = 0
        for dext in dexts:
            if dext['mirror_status'] == vas_db.MIRROR_STATUS['INSYNC']:
                count += 1
        return count

    def __get_resync_load_hsvr(self, hsvrid):
        if self.resync_load_hsvr.has_key(hsvrid):
            return self.resync_load_hsvr[hsvrid]
        else:
            return 0 
    def __get_resync_load_pdsk(self, pdskid):
        if self.resync_load_pdsk.has_key(pdskid):
            return self.resync_load_pdsk[pdskid]
        else:
            return 0 
    def __get_resync_load_ssvr(self, ssvrid):
        if self.resync_load_ssvr.has_key(ssvrid):
            return self.resync_load_ssvr[ssvrid]
        else:
            return 0 

    def __inc_resync_load(self, hsvrid, pdskid, ssvrid):
        if self.resync_load_hsvr.has_key(hsvrid):
            self.resync_load_hsvr[hsvrid] += 1
        else:
            self.resync_load_hsvr[hsvrid] = 1
        if self.resync_load_pdsk.has_key(pdskid):
            self.resync_load_pdsk[pdskid] += 1
        else:
            self.resync_load_pdsk[pdskid] = 1
        if self.resync_load_ssvr.has_key(ssvrid):
            self.resync_load_ssvr[ssvrid] += 1
        else:
            self.resync_load_ssvr[ssvrid] = 1

    def __dec_resync_load(self, hsvrid, pdskid, ssvrid):
        assert(self.resync_load_hsvr.has_key(hsvrid))
        self.resync_load_hsvr[hsvrid] -= 1
        assert(self.resync_load_pdsk.has_key(pdskid))
        self.resync_load_pdsk[pdskid] -= 1
        assert(self.resync_load_ssvr.has_key(ssvrid))
        self.resync_load_ssvr[ssvrid] -= 1

    def __salvage_resync(self):
        # this is called in storage_manager initialization only. so skip main_lock
        remain = self.db.resync.rowcount()
        if remain == 0:
            return
        resync_rows = self.db.c.fetchall()
        self.db.begin_transaction()
        try:
            for resync in resync_rows:
                if resync['status'] == EVENT_STATUS['PROGRESS']:
                    self.db.resync.update_value('eventid', resync['eventid'], \
                        'status', EVENT_STATUS['PENDING'])        
                elif resync['status'] == EVENT_STATUS['CANCELED']:
                    self.db.resync.delete_rows('eventid', resync['eventid'])
                else:
                    # assert status == PENDING
                    pass
            self.db.commit()
        except:
            # XXX what to do
            logger.error(traceback.format_exc())
            self.db.rollback()

    def __resync_worker(self):
        # TODO .sm_db_vol change case: __putDbInfoFile ==>  thread
        logger.debug("resync_worker start.")
        self.resync_load_hsvr = {}
        self.resync_load_pdsk = {}
        self.resync_load_ssvr = {}
        self.resync_progress = {}

        self.resync_cv.acquire()
        logger.debug("resync_worker acquire.")
        while True:
            logger.debug("resync_worker loop start.")
            try:
                resync_to_do = self.__get_resync_to_do(MAX_RESYNC_TASKS)
                logger.debug("resync_to_do: %s" % (resync_to_do))
                for eventid, mirrorid, lvolid_add, lvolid_rm, hsvrid, pdskid, ssvrid in resync_to_do:
                    th = threading.Thread(target = self.__request_and_poll_resync, \
                        args = (eventid, hsvrid, mirrorid, lvolid_add, lvolid_rm))
                    th.setDaemon(True)
                    th.start()
                    self.resync_progress[eventid] = (hsvrid, pdskid, ssvrid)
                    self.__inc_resync_load(hsvrid, pdskid, ssvrid)
                    self.db.begin_transaction()
                    self.db.resync.update_value('eventid', eventid, 'status', EVENT_STATUS['PROGRESS'])
                    self.db.commit()
            except:
                logger.error(traceback.format_exc())
                pass # XXX
            logger.debug("resync_worker loop end. wait.")
            self.resync_cv.wait()

    def __get_resync_load_from_dextid(self, dextid):
        dskmap_row = self.db.dskmap.get_row(dextid)
        pdsklst_row = self.db.pdsklst.get_row(dskmap_row['pdskid'])
        dext = self.db.lvolmap.get_row(dextid)
        attach = lvol_get_attach(self.db, dext['toplvolid'])
        assert(attach), lineno()
        return (attach['hsvrid'], self.__get_resync_load_hsvr(attach['hsvrid']), \
            dskmap_row['pdskid'], self.__get_resync_load_pdsk(dskmap_row['pdskid']), \
            pdsklst_row['ssvrid'], self.__get_resync_load_ssvr(pdsklst_row['ssvrid']))

    def __get_resync_to_do(self, count):
        ret = []
        logger.debug("__get_resync_to_do start. %d progress: %d" % (count, len(self.resync_progress)))
        # limit number of resync tasks run simulataneously
        if len(self.resync_progress) >= count:
            return ret
        count -= len(self.resync_progress)

        # choose resync tasks and build requests for an hsvr_agent
        resyncs_to_do = self.db.resync.get_rows('status', EVENT_STATUS['PENDING'])
        logger.debug("__get_resync_to_do pending %d" % (len(resyncs_to_do)))
        if len(resyncs_to_do) < count:
            count = len(resyncs_to_do)
        if len(resyncs_to_do) == 0:
            return ret
        
        # count valid copy for each dexts
        priority_points = []
        records = {}
        disk_semaphore = {}
        for task in resyncs_to_do:
            eventid = task['eventid']
            dextid = task['lvolid_add']
            hsvrid, hload, pdskid, pload, ssvrid, sload = self.__get_resync_load_from_dextid(dextid)
            # check ssvr is online
            ssvr = self.db.ssvrlst.get_row(ssvrid)
            if not ssvr or ssvr['priority'] != vas_db.ALLOC_PRIORITY['HIGH']:
                continue
            # do not put more load on disks under resync
            if pload > 0:
                continue
            point = self.__count_valid_copy(dextid) * 10 + hload * 1 + sload * 1
            priority_points.append((point, eventid))
            records[eventid] = (eventid, task['mirrorid'], dextid, task['lvolid_rm'], hsvrid, pdskid, ssvrid)
            disk_semaphore[pdskid] = 0

        # build and send resync request for hsvr_agent
        for _point, eventid in sorted(priority_points):
            _, _, _, _, _, pdskid, _ = records[eventid]
            if disk_semaphore[pdskid]:  # do not execute more than one resync on a disk
                continue
            else:
                count -= 1
                disk_semaphore[pdskid] = 1
            ret.append(records[eventid])
            if count <= 0:
                break
        return ret

    def __request_and_poll_resync(self, eventid, hsvrid, mirrorid, lvolid_add, lvolid_rm):
        def resync_end():
            hsvrid, pdskid, ssvrid = self.resync_progress[eventid]
            self.__dec_resync_load(hsvrid, pdskid, ssvrid)
            del self.resync_progress[eventid]

        def make_arg(): # TODO shorten code
            main_lock.acquire()
            try: 
                dext = self.db.dskmap.get_row(lvolid_add)
                dsk = self.db.pdsklst.get_row(dext['pdskid'])
                ip_addrs = self.db.ipdata.ssvr_ipdata(dsk['ssvrid'])
                ip_paths = []
                for ip in ip_addrs:
                    path = get_iscsi_path(ip, dext['pdskid'], dsk['srp_name'])
                    ip_paths.append(path)
                lvolspec_add = { 'pdskid': 0, 'offset': dext['offset'], 'ssvrid': dsk['ssvrid'], \
                    'iscsi_path': ip_paths}
                lvolstruct_add = { 'lvolid': lvolid_add, 'lvoltype': vas_db.LVOLTYPE['DEXT'], \
                    'capacity': dext['capacity'], 'lvolspec': lvolspec_add, 'labels': [], \
                    'mirror_status': 0}

                if lvolid_rm == lvolid_add:
                    lvolstruct_remove = lvolstruct_add
                elif lvolid_rm == 0:
                    lvolstruct_remove = { 'lvolid': 0, 'lvoltype': vas_db.LVOLTYPE['DEXT'], \
                        'capacity': 0, 'lvolspec': {}, 'labels': [], 'mirror_status': 0}
                else:
                    dext = self.db.dskmap.get_row(lvolid_rm)
                    dsk = self.db.pdsklst.get_row(dext['pdskid'])
                    ip_addrs = self.db.ipdata.ssvr_ipdata(dsk['ssvrid'])
                    ip_paths = []
                    for ip in ip_addrs:
                        path = get_iscsi_path(ip, dext['pdskid'], dsk['srp_name'])
                        ip_paths.append(path)
                    lvolspec_remove = { 'pdskid': 0, 'offset': 0, 'ssvrid': dsk['ssvrid'], \
                        'iscsi_path': ip_paths}
                    lvolstruct_remove = { 'lvolid': lvolid_rm, 'lvoltype': vas_db.LVOLTYPE['DEXT'], \
                        'capacity': 0, 'lvolspec': lvolspec_remove, 'labels': [], \
                        'mirror_status': 0}

                subargs = {'ver': XMLRPC_VERSION, 'eventid': eventid, 'mirrorid': mirrorid, \
                    'add': lvolstruct_add, 'remove': lvolstruct_remove}

                hsvrid = self.__mirrorid_to_hsvrid(mirrorid)
                ip_addrs = self.db.ipdata.hsvr_ipdata(hsvrid)
            except Exception:
                # only possible case is that volume was detached
                logger.debug(traceback.format_exc())
                main_lock.release()
                raise
            main_lock.release()
            return ip_addrs, subargs

        def retry_resync():
            # resync record maybe going to delete. wait a while
            time.sleep(10)
            # back to PENDING and reschedule
            main_lock.acquire()
            try:
                self.db.begin_transaction()
                # note: resync record may be deleted
                self.db.resync.update_value('eventid', eventid, 'status', EVENT_STATUS['PENDING'])
                self.db.commit()    
                resync_end()
                self.resync_cv.notifyAll()
            except: # should not occur
                logger.error(traceback.format_exc())
                self.db.rollback()
            main_lock.release()

        try:
            ip_addrs, subargs = make_arg()
            self.__send_request(ip_addrs, port_hsvr_agent, "replaceMirrorDisk", subargs)
        except Exception, inst:
            logger.error('resync replaceMirrorDisk fail (%d, %d, %d): %s' % \
                (hsvrid, mirrorid, lvolid_add, inst))
            # maybe hsvr_agent down, so retry. 
            # if volume detached or host down, resync record will be deleted anyway.
            retry_resync()
            return

        time.sleep(60) # TODO define constant
        try:
            status, _ = self.__poll_event(ip_addrs, port_hsvr_agent, eventid, 10, "resync")
        except Exception, inst:
            # log message was output already
            # maybe hsvr_agent down, so retry. 
            # if volume detached or host down, resync record will be deleted anyway.
            retry_resync()
            return

        if status == EVENT_STATUS['NONE']:
            logger.debug("resync request missing %s" % (subargs))
            # hsvr_agent restart, so retry.
            retry_resync()
            return
        elif status == EVENT_STATUS['ERROR']:
            logger.debug("resync request error %s" % (subargs))
            retry_resync() # resync record may be deleted already anyway
            return
        # else: DONE

        # OK rebuild finished
        logger.info('rebuild finished (%d, %d, %d)' % (hsvrid, mirrorid, lvolid_add))
        main_lock.acquire()
        try:
            self.db.begin_transaction()
            # resync record may be deleted
            if self.db.resync.get_row(eventid):
                self.db.rebuildfinished_mirror(lvolid_add)
                self.db.resync.delete_rows('eventid', eventid)
            self.db.commit()    
            resync_end()
            self.resync_cv.notifyAll()
            self.shred_cv.notifyAll()
        except:
            logger.error(traceback.format_exc())
            self.db.rollback()
        main_lock.release()

    def __assert_dsk_offline(self, pdskid):
        dsk = self.db.pdsklst.get_row(pdskid)
        assert(dsk), lineno()
        if dsk['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'] ,vas_db.ALLOC_PRIORITY['FAULTY']):
            raise Exception, "__assert_dsk_offline: physical disk(pdsk-%08x) is not offline" % pdskid

    def __assert_dext_offline(self, lvolid):
        dskmap = self.db.dskmap.get_row(lvolid)
        assert(dskmap), lineno()
        if dskmap['status'] not in (vas_db.EXT_STATUS['OFFLINE'], vas_db.EXT_STATUS['FAULTY'], vas_db.EXT_STATUS['FREE'], vas_db.EXT_STATUS['SUPER']):
            raise Exception, "__assert_dext_offline: dext(dext-%08x) is not offline" % lvolid
        lvmp = self.db.lvolmap.get_row(lvolid)
        if lvmp:
            raise Exception, "__assert_dext_offline: dext(dext-%08x) is allocated" % lvolid

    def __delete_offline_disk(self, pdskid):
        self.__assert_dsk_offline(pdskid)

        dskmaps = self.db.dskmap.get_rows('pdskid', pdskid)
        for dskmap in dskmaps:
            self.__assert_dext_offline(dskmap['dextid'])

        self.db.pdsklst.delete_rows('pdskid', pdskid)
        self.db.dskmap.delete_rows('pdskid', pdskid)

    def createLogicalVolume(self, data):

        try:
            redundancy = 3;
            if data.has_key('redundancy'):
                redundancy = data['redundancy']
            if redundancy < MIN_REDUNDANCY or redundancy > MAX_REDUNDANCY:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            try:
                lvolname = data['lvolname']
                check_lvolname(lvolname)
            except:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            if not data.has_key('capacity') or \
              type(data['capacity']) != IntType:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            capacity = getRoundUpCapacity(data['capacity'])
            if capacity < 1 or capacity > MAX_LENGTH :
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            if self.db.lvollst.get_rows('lvolname', lvolname):
                # volume already exists
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')

            lvinfo = {}
            lvinfo['lvolname'] = lvolname
            lvinfo['redundancy'] = redundancy
            lvinfo['capacity'] = capacity

            # make lvol db create chain
            # lvol_db_create does not know lvol structure.
            # make chain outside of lvol_db_create
            node = top = SnapshotOriginDbNode(None)
            node = LinearDbNode(node)
            node = MirrorDbNode(node)
            DextDbNode(node)

            try:
                self.db.begin_transaction()
                lvolid = lvol_db_create(self.db, top, lvinfo)
                self.db.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return lvolid

    def createSnapshot(self, data):
        def get_snapshot_lvolid(lvolstruct, type):
            assert(lvolstruct['lvoltype'] == vas_db.LVOLTYPE['LVOL'])
            assert(len(lvolstruct['components']) == 1)
            c = lvolstruct['components'][0]
            assert(c['lvoltype'] == type)
            return c['lvolid']
        def calc_cow_size(size_in_gb):
            def div_up(a, b):
                return (a + b - 1) / b
            chunk_size = SNAPSHOT_CHUNK_SIZE
            sizeof_disk_exception = 16
            exceptions_per_area = chunk_size * 512 / sizeof_disk_exception
            area_size_in_chunk = exceptions_per_area + 1

            # for each chunk_size * exceptions_per_area logical space,
            # dm-snapshot consumes up to chunk_size * area_size_in_chunk
            # physical space.
            #
            # besides, a chunk_size physical space is used for
            # the on-disk header.

            size_in_bytes = size_in_gb * 1024 * 1024 * 1024
            narea = div_up(size_in_bytes, chunk_size * exceptions_per_area)
            cow_size_in_bytes = \
                chunk_size * area_size_in_chunk * narea + chunk_size
            cow_size_in_gb = div_up(cow_size_in_bytes, 1024 * 1024 * 1024)
            return cow_size_in_gb

        try:
            origin_lvol = self.__get_lvol_by_id_or_name(data, 'origin_')
            origin_lvolid = origin_lvol['lvolid']
            try:
                snapshot_lvolname = data['snapshot_lvolname']
                check_lvolname(snapshot_lvolname)
            except:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if self.db.lvollst.get_rows('lvolname', snapshot_lvolname):
                # volume already exists
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')

            attach = lvol_get_attach(self.db, origin_lvolid)
            if attach and attach['status'] != vas_db.ATTACH_STATUS['BOUND']:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            root_lvolid = lvol_find_assoc_root(self.db, origin_lvolid)

            origin_lvolstruct = get_lvolstruct(self.db, origin_lvolid)
            assert(origin_lvolstruct['lvoltype'] == vas_db.LVOLTYPE['LVOL'])
            assert(len(origin_lvolstruct['components']) == 1)
            c = origin_lvolstruct['components'][0]
            if c['lvoltype'] != vas_db.LVOLTYPE['SNAPSHOT-ORIGIN']:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            origin_lvolspec = origin_lvolstruct['lvolspec']
            origin_snapshot_lvolid = get_snapshot_lvolid(origin_lvolstruct, \
                vas_db.LVOLTYPE['SNAPSHOT-ORIGIN'])

            lvinfo = {}
            lvinfo['lvolname'] = snapshot_lvolname
            lvinfo['redundancy'] = origin_lvolspec['redundancy']
            lvinfo['capacity'] = \
                getRoundUpCapacity(calc_cow_size(origin_lvolstruct['capacity']))

            node = top = SnapshotDbNode(None)
            node = LinearDbNode(node)
            node = MirrorDbNode(node)
            DextDbNode(node)

            try:
                self.db.begin_transaction()
                snapshot_lvolid = lvol_db_create(self.db, top, lvinfo)
                snapshot_lvolstruct = get_lvolstruct(self.db, snapshot_lvolid)
                snapshot_snapshot_lvolid = get_snapshot_lvolid( \
                    snapshot_lvolstruct, vas_db.LVOLTYPE['SNAPSHOT'])
                self.db.snapshot.put_row( \
                    (origin_snapshot_lvolid, snapshot_snapshot_lvolid))
                lvol_db_assoc(self.db, origin_lvolid, snapshot_lvolid, \
                    'snapshot')
                if attach:
                    eventid = self.db.genid_event()
                    self.db.attach.set_status(root_lvolid, \
                        vas_db.ATTACH_STATUS['SNAPSHOT-BINDING'])
                    self.db.attach.update_value('lvolid', root_lvolid, \
                        'eventid', eventid)
                    self.db.attach.update_value('lvolid', root_lvolid, \
                        'auxdata', snapshot_lvolid)
                self.db.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            if attach:
                self.__lvol_attach(snapshot_lvolid, attach['hsvrid'], \
                    attach['status'], eventid)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return snapshot_lvolid

    def createSnapshot(self, data):
        def get_snapshot_lvolid(lvolstruct, type):
            assert(lvolstruct['lvoltype'] == vas_db.LVOLTYPE['LVOL'])
            assert(len(lvolstruct['components']) == 1)
            c = lvolstruct['components'][0]
            assert(c['lvoltype'] == type)
            return c['lvolid']
        def calc_cow_size(size_in_gb):
            def div_up(a, b):
                return (a + b - 1) / b
            chunk_size = SNAPSHOT_CHUNK_SIZE
            sizeof_disk_exception = 16
            exceptions_per_area = chunk_size * 512 / sizeof_disk_exception
            area_size_in_chunk = exceptions_per_area + 1

            # for each chunk_size * exceptions_per_area logical space,
            # dm-snapshot consumes up to chunk_size * area_size_in_chunk
            # physical space.
            #
            # besides, a chunk_size physical space is used for
            # the on-disk header.

            size_in_bytes = size_in_gb * 1024 * 1024 * 1024
            narea = div_up(size_in_bytes, chunk_size * exceptions_per_area)
            cow_size_in_bytes = \
                chunk_size * area_size_in_chunk * narea + chunk_size
            cow_size_in_gb = div_up(cow_size_in_bytes, 1024 * 1024 * 1024)
            return cow_size_in_gb

        try:
            origin_lvol = self.__get_lvol_by_id_or_name(data, 'origin_')
            origin_lvolid = origin_lvol['lvolid']
            try:
                snapshot_lvolname = data['snapshot_lvolname']
                check_lvolname(snapshot_lvolname)
            except:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if self.db.lvollst.get_rows('lvolname', snapshot_lvolname):
                # volume already exists
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')

            attach = lvol_get_attach(self.db, origin_lvolid)
            if attach and \
                (attach['eventid'] or attach['status'] != vas_db.ATTACH_STATUS['BOUND']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            assert(origin_lvolid == lvol_find_assoc_root(self.db, origin_lvolid))

            origin_lvolstruct = get_lvolstruct(self.db, origin_lvolid)
            assert(origin_lvolstruct['lvoltype'] == vas_db.LVOLTYPE['LVOL'])
            assert(len(origin_lvolstruct['components']) == 1)
            c = origin_lvolstruct['components'][0]
            if c['lvoltype'] != vas_db.LVOLTYPE['SNAPSHOT-ORIGIN']:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            origin_lvolspec = origin_lvolstruct['lvolspec']
            origin_snapshot_lvolid = get_snapshot_lvolid(origin_lvolstruct, \
                vas_db.LVOLTYPE['SNAPSHOT-ORIGIN'])

            lvinfo = {}
            lvinfo['lvolname'] = snapshot_lvolname
            lvinfo['redundancy'] = origin_lvolspec['redundancy']
            lvinfo['capacity'] = \
                getRoundUpCapacity(calc_cow_size(origin_lvolstruct['capacity']))

            node = top = SnapshotDbNode(None)
            node = LinearDbNode(node)
            node = MirrorDbNode(node)
            DextDbNode(node)

            try:
                self.db.begin_transaction()
                snapshot_lvolid = lvol_db_create(self.db, top, lvinfo)
                snapshot_lvolstruct = get_lvolstruct(self.db, snapshot_lvolid)
                snapshot_snapshot_lvolid = get_snapshot_lvolid( \
                    snapshot_lvolstruct, vas_db.LVOLTYPE['SNAPSHOT'])
                self.db.snapshot.put_row( \
                    (origin_snapshot_lvolid, snapshot_snapshot_lvolid))
                lvol_db_assoc(self.db, origin_lvolid, snapshot_lvolid, \
                    'snapshot')
                if attach:
                    eventid = self.db.genid_event()
                    self.db.attach.start_event(eventid, origin_lvolid, \
                        snapshot_lvolid, vas_db.ATTACH_EVENT['BINDING'])
                self.db.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            if attach:
                self.__lvol_attach(snapshot_lvolid, attach['hsvrid'], \
                    eventid)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return snapshot_lvolid

    def __get_shred_load_pdsk(self, pdskid):
        if self.shred_load_pdsk.has_key(pdskid):
            return self.shred_load_pdsk[pdskid]
        else:
            return 0 
    def __get_shred_load_ssvr(self, ssvrid):
        if self.shred_load_ssvr.has_key(ssvrid):
            return self.shred_load_ssvr[ssvrid]
        else:
            return 0 

    def __inc_shred_load_pdsk(self, pdskid):
        if self.shred_load_pdsk.has_key(pdskid):
            self.shred_load_pdsk[pdskid] += 1
        else:
            self.shred_load_pdsk[pdskid] = 1
    def __inc_shred_load_ssvr(self, ssvrid):
        if self.shred_load_ssvr.has_key(ssvrid):
            self.shred_load_ssvr[ssvrid] += 1
        else:
            self.shred_load_ssvr[ssvrid] = 1

    def __dec_shred_load_pdsk(self, pdskid):
        assert(self.shred_load_pdsk.has_key(pdskid))
        self.shred_load_pdsk[pdskid] -= 1
    def __dec_shred_load_ssvr(self, ssvrid):
        assert(self.shred_load_ssvr.has_key(ssvrid))
        self.shred_load_ssvr[ssvrid] -= 1

    def __salvage_shred(self):
        # this is called in storage_manager initialization only. so skip main_lock
        remain = self.db.shred.rowcount()
        if remain == 0:
            return
        shred_rows = self.db.c.fetchall()
        self.db.begin_transaction()
        try:
            for shred in shred_rows:
                if shred['status'] == EVENT_STATUS['PROGRESS']:
                    self.db.shred.update_value('eventid', shred['eventid'], \
                        'status', EVENT_STATUS['PENDING'])        
                elif shred['status'] == EVENT_STATUS['CANCELED']:
                    self.db.shred.delete_rows('eventid', shred['eventid'])
                else:
                    # assert status == PENDING
                    pass
            self.db.commit()
        except:
            # XXX what to do
            logger.error(traceback.format_exc())
            self.db.rollback()

    def __shred_worker(self):
        logger.debug("shred_worker start.")
        self.shred_load_pdsk = {}
        self.shred_load_ssvr = {}
        self.shred_progress = {}

        self.shred_cv.acquire()
        logger.debug("shred_worker acquire.")
        while True:
            logger.debug("shred_worker loop start.")
            try:
                shred_to_do = self.__get_shred_to_do(MAX_RESYNC_TASKS)
                logger.debug("shred_to_do: %s" % (shred_to_do))
                for eventid, dextid, pdskid, offset, capacity, ssvrid in shred_to_do:
                    ip_addrs = self.db.ipdata.ssvr_ipdata(ssvrid)
                    th = threading.Thread(target = self.__request_and_poll_shred, \
                        args = (eventid, dextid, pdskid, offset, capacity, ssvrid, ip_addrs))
                    th.setDaemon(True)
                    th.start()
                    self.shred_progress[eventid] = (pdskid, ssvrid)
                    self.db.begin_transaction()
                    self.db.shred.update_value('eventid', eventid, 'status', EVENT_STATUS['PROGRESS'])
                    self.db.commit()
            except:
                logger.error(traceback.format_exc())
                pass # XXX
            logger.debug("shred_worker loop end. wait.")
            self.shred_cv.wait()

    def __get_shred_to_do(self, count):
        ret = []
        logger.debug("__get_shred_to_do start. %d progress: %d" % (count, len(self.shred_progress)))
        # limit number of shred tasks run simulataneously
        if len(self.shred_progress) >= count:
            return ret
        count -= len(self.shred_progress)

        shreds_to_do = self.db.shred.get_rows('status', EVENT_STATUS['PENDING'])
        if len(shreds_to_do) < count:
            count = len(shreds_to_do)
        if len(shreds_to_do) == 0:
            return ret
        
        # count valid copy for each dexts
        priority_points = []
        records = {}
        for task in shreds_to_do:
            eventid = task['eventid']
            dextid = task['dextid']
            ssvrid = self.__dextid_to_ssvrid(dextid)
            pdskid = self.__dextid_to_pdskid(dextid)

            # do not put more load on disks under resync
            if self.__get_resync_load_pdsk(pdskid) > 0:
                continue

            # do not put more load on disks under shred
            if self.__get_shred_load_pdsk(pdskid) >= MAX_SHRED_TASKS_PER_DISK:
#                logger.debug("__get_shred_to_do passed for load disk:  eventid: %d dextid: %d pdskid: %d" % \
#                    (eventid, dextid, pdskid))
                continue
            if self.__get_shred_load_ssvr(ssvrid) >= SHREDDER_COUNT:
#                logger.debug("__get_shred_to_do passed for load ssvr:  eventid: %d dextid: %d pdskid: %d" % \
#                    (eventid, dextid, pdskid))
                continue
            self.__inc_shred_load_pdsk(pdskid)
            self.__inc_shred_load_ssvr(ssvrid)

            point = self.__get_resync_load_ssvr(ssvrid) + self.__get_shred_load_ssvr(ssvrid)
            priority_points.append((point, eventid))
            records[eventid] = (eventid, dextid, pdskid, task['offset'], task['capacity'], ssvrid)

        # build and send resync request for ssvr_agent
        for _point, eventid in sorted(priority_points):
            if count > 0:
                ret.append(records[eventid])
                count -= 1
            else:
                # back to load count upped above
                _, _, pdskid, _, _, ssvrid = records[eventid]
                self.__dec_shred_load_pdsk(pdskid)
                self.__dec_shred_load_ssvr(ssvrid)

        return ret

    def __request_and_poll_shred(self, eventid, dextid, pdskid, offset, capacity, ssvrid, ip_addrs):
        logger.debug("__request_and_poll_shred call registerShredRequest. %d" % dextid)
        def retry_shred():
            # delete if CANCELED, otherwise back to PENDING and reschedule
            main_lock.acquire()
            try:
                self.db.begin_transaction()
                logger.debug("shred retry")
                shred_row = self.db.shred.get_row(eventid)
                if shred_row['status'] == EVENT_STATUS['CANCELED']:
                    self.db.shred.delete_rows('eventid', eventid)
                else: # assert status == PROGRESS
                    self.db.shred.update_value('eventid', eventid, 'status', EVENT_STATUS['PENDING'])
                self.__dec_shred_load_pdsk(pdskid)
                self.__dec_shred_load_ssvr(ssvrid)
                del self.shred_progress[eventid]
                self.shred_cv.notifyAll()
                self.db.commit()
            except: # should not occur
                logger.error(traceback.format_exc())
                self.db.rollback()
            main_lock.release()

        subargs = {'ver': XMLRPC_VERSION, 'eventid': eventid, 'dextid': dextid, \
            'offset': offset, 'capacity': capacity, 'pdskid': pdskid}
        try:
            self.__send_request(ip_addrs, port_ssvr_agent, "registerShredRequest", subargs)
        except Exception, inst:
            logger.error('shred registerShredRequest fail (%d): %s' % (dextid, inst))
            retry_shred()
            return

        try:
            status, _ = self.__poll_event(ip_addrs, port_ssvr_agent, eventid, 60, "shred")
        except: # log message was output already
            retry_shred()
            return

        if status != EVENT_STATUS['DONE']: # NONE or ERROR
            logger.debug("shred request missing or error %s" % (subargs))
            retry_shred()
            return

        # done
        main_lock.acquire()
        try:
            self.db.begin_transaction()
            dskmap = self.db.dskmap.get_row(dextid)
            if dskmap['status'] == vas_db.EXT_STATUS['OFFLINE']:
                self.db.dskmap.update_value('dextid', dextid, 'status', vas_db.EXT_STATUS['FREE'])
            self.db.shred.delete_rows('eventid', eventid)
            self.__dec_shred_load_pdsk(pdskid)
            self.__dec_shred_load_ssvr(ssvrid)
            del self.shred_progress[eventid]
            self.shred_cv.notifyAll()
            self.db.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db.rollback()
        main_lock.release()

    def __is_snapshot(self, lvolid):
        assoc = self.db.assoc.get_rows('assoc_lvolid', lvolid)
        if assoc:
            assert(len(assoc) == 1), lineno()
            if assoc[0]['type'] == "snapshot":
                return True
        return False

    def deleteLogicalVolume(self, data):
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            lvolid = lvol['lvolid']
            if self.db.assoc.get_rows('lvolid', lvolid):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            attach = lvol_get_attach(self.db, lvolid)
            if attach:
                if self.__is_snapshot(lvolid):
                    if attach['eventid'] or attach['status'] != vas_db.ATTACH_STATUS['BOUND']:
                        raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
                    root_lvolid = lvol_find_assoc_root(self.db, lvolid)
                    # will be deleted from DB after detach
                    self.__lvol_detach(root_lvolid, lvolid, attach['hsvrid'])
                    return 0
                else:
                    raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            if self.db.assoc.get_rows('lvolid', lvolid):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            try:
                self.db.begin_transaction()
                lvol_db_delete(self.db, lvolid)
                self.db.commit()
                self.shred_cv.notifyAll()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __lvolops_worker(self, ip_addrs, eventid, method, lvolstruct, lvolops_done):
        lvolid = lvolstruct['lvolid']
        root_lvolid = lvol_find_assoc_root(self.db, lvolid)
        logger.debug("lvolops_worker start %s %d %d %d" % (method, eventid, root_lvolid, lvolid))

        def __check_attach_gone():
            if not self.db.attach.get_row(root_lvolid):
                self.__notify_lvolops_done(eventid, -1, \
                    xmlrpclibFault(errno.EHOSTDOWN, "EHOSTDOWN"))
                return True
            return False
            
        def check_attach_gone():
            end = False
            main_lock.acquire()
            try:
                end = __check_attach_gone()
            except:
                logger.error(traceback.format_exc())
            main_lock.release()
            return end

        def cancel(mark_error):
            main_lock.acquire()
            try:
                if not __check_attach_gone():
                    self.db.begin_transaction()
                    try:
                        if mark_error:
                            self.db.attach.end_event_error(eventid, root_lvolid)
                        else:
                            self.db.attach.end_event_cancel(eventid, root_lvolid)
                        self.db.commit()
                    except:
                        logger.error(traceback.format_exc())
                        self.db.rollback()
                    self.__notify_lvolops_done(eventid, -1, inst)
            except:
                logger.error(traceback.format_exc())
            main_lock.release()

        while True:
            if check_attach_gone():
                return

            try:
                subargs = {'ver': XMLRPC_VERSION, 'eventid': eventid, 'lvolstruct': lvolstruct}
                self.__send_request(ip_addrs, port_hsvr_agent, method, subargs)
            except xmlrpclib.Fault, inst:
                if inst.faultCode == errno.ETIMEDOUT:
                    logger.debug("lvolops_worker request %s timedout, retry" % (method))
                    continue
                logger.debug("lvolops_worker request %s failed: %s" % (method, inst))
                cancel(False)
                return
            except Exception, inst:
                logger.debug("lvolops_worker request %s failed: %s" % (method, inst))
                cancel(False)
                return

            try:
                status, result = self.__poll_event(ip_addrs, port_hsvr_agent, eventid, \
                    3, "attach") # TODO define interval value as constant
            except Exception, inst:
                # log message was output already
                cancel(True) # mark ERROR
                return 

            if status == EVENT_STATUS['NONE']:
                # hsvr_agent restart. try attach request again.
                continue
            elif status == EVENT_STATUS['ERROR']:
                # result is error code 
                inst = xmlrpclib.Fault(result, "XXX") # XXX: fix error string
                if method == "detachLogicalVolume" and result == errno.EBUSY:
                    # volume is used.
                    # note that hsvr_agent must not return EBUSY if volume is not healthy
                    cancel(False)
                else:
                    cancel(True) # mark ERROR
                return
            else: # DONE
                assert(status == EVENT_STATUS['DONE']), lineno()
                break
        # done
        main_lock.acquire()
        try:
            if not __check_attach_gone():
                lvolops_done(eventid, root_lvolid, lvolid)
                self.__notify_lvolops_done(eventid, 0, None)
        except Exception, inst:
            # code bug
            logger.error(traceback.format_exc())
        main_lock.release()

    def __lvol_attach_done(self, eventid, root_lvolid, lvolid):
        self.db.begin_transaction()
        try:
            self.db.attach.end_event_success(eventid, root_lvolid)
            lvol_db_attach(self.db, lvolid)
            self.db.commit()
            # resync record may be added while lvol_db_attach
            self.resync_cv.notifyAll()
        except Exception, inst:
            # should not occur
            # user can use volume but DB remain BINDING. this can be fixed to do detach
            # (if no one use volume)
            logger.error(traceback.format_exc())
            self.db.rollback()

    def __lvol_detach_done(self, eventid, root_lvolid, lvolid):
        self.db.begin_transaction()
        try:
            self.db.attach.end_event_success(eventid, root_lvolid)
            lvol_db_detach(self.db, lvolid)
            self.db.commit()
            self.shred_cv.notifyAll()
        except Exception, inst:
            # should not occur
            # remain UNBINDING. but this can be fix to do detach again
            logger.error(traceback.format_exc())
            self.db.rollback()

    def __attachLogicalVolume(self, lvolid, hsvrid):
        if self.db.assoc.get_rows('assoc_lvolid', lvolid):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        assert(lvolid == lvol_find_assoc_root(self.db, lvolid))
        attach = lvol_get_attach(self.db, lvolid)
        if attach:
            raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
        hsvrlst = self.db.hsvrlst.get_row(hsvrid)
        if not hsvrlst:
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        if hsvrlst['priority'] in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        try:
            self.db.begin_transaction()
            eventid = self.db.genid_event()
            self.db.attach.put_row((lvolid, hsvrid, vas_db.ATTACH_STATUS['BOUND'], \
                eventid, vas_db.ATTACH_EVENT['BINDING'], 0))
            self.db.commit()
        except:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise
        self.__lvol_attach(lvolid, hsvrid, eventid)

    def __lvol_attach(self, lvolid, hsvrid, eventid):
        lvolstruct = get_lvolstruct(self.db, lvolid)
        ip_addrs = self.db.ipdata.hsvr_ipdata(hsvrid)
        cv = threading.Condition(main_lock)
        self.attach_table[eventid] = {'cv': cv}
        start_worker(self.__lvolops_worker, ip_addrs, eventid, \
            "attachLogicalVolume", lvolstruct, self.__lvol_attach_done)
        cv.wait() # release main_lock and acquire again

        result = self.attach_table[eventid]
        del self.attach_table[eventid]
        if result['status'] < 0:
            raise result['ex']

    def __lvol_detach(self, root_lvolid, lvolid, hsvrid):
        lvolstruct = get_lvolstruct(self.db, lvolid)
        ip_addrs = self.db.ipdata.hsvr_ipdata(hsvrid)
        assert(ip_addrs)

        try:
            self.db.begin_transaction()
            eventid = self.db.genid_event()
            self.db.attach.start_event(eventid, root_lvolid, lvolid, \
                vas_db.ATTACH_EVENT['UNBINDING']) 
            if root_lvolid != lvolid:
                self.db.lvollst.update_value('lvolid', lvolid, 'deleted', 1)
            self.db.commit()
        except:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise

        cv = threading.Condition(main_lock)
        self.attach_table[eventid] = {'cv': cv}
        start_worker(self.__lvolops_worker, ip_addrs, eventid, \
            "detachLogicalVolume", lvolstruct, self.__lvol_detach_done)
        cv.wait() # release main_lock and acquire again

        result = self.attach_table[eventid]
        del self.attach_table[eventid]
        if result['status'] < 0:
            raise result['ex']

    def __poll_event(self, ip_addrs, port, eventid, interval, label = ""):
        # caller should handle exception 
        subargs = {'ver': XMLRPC_VERSION, 'eventid': eventid}
        while True:
            # some event complete immediately, so get event status at first
            try:
                status, result = self.__send_request(ip_addrs, port, "getEventStatus", subargs)
            except xmlrpclib.Fault, inst:
                if inst.faultCode == errno.ETIMEDOUT:
                    logger.debug("lvolops_worker request %s timedout, retry" % (method))
                    continue
                logger.error("%s getEventStatus %d failed: %s" % (label, eventid, inst))
                raise
            except Exception, inst:
                logger.error("%s getEventStatus %d failed: %s" % (label, eventid, inst))
                raise
            if status != EVENT_STATUS['PROGRESS']:
                break;
            logger.debug("%s event %d still progress" % (label, eventid))
            time.sleep(interval)
        logger.debug("%s event %d done. (%d, %d)" % (label, eventid, status, result))
        return status, result

    def __notify_lvolops_done(self, eventid, st, ex):
        if self.attach_table.has_key(eventid):
            t = self.attach_table[eventid]
            t['status'] = st
            t['ex'] = ex
            t['cv'].notifyAll()
            # notified thread delete table

    def __get_lvol_by_id_or_name(self, data, prefix = ''):
        lvol = None
        lvolid_key = prefix + 'lvolid'
        lvolname_key = prefix + 'lvolname'
        if data.has_key(lvolid_key) and data[lvolid_key]:
            lvol = self.db.lvollst.get_row(data[lvolid_key])
        elif data.has_key(lvolname_key) and data[lvolname_key]:
            lvols = self.db.lvollst.get_rows('lvolname', data[lvolname_key])
            assert(len(lvols) <= 1)
            if len(lvols) == 1:
                lvol = lvols[0]
        else:
            # both lvolid and lvolname are not specified
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        if not lvol:
            # volume not exists
            raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
        return lvol

    def attachLogicalVolume(self, data):
        hsvrid = mand_keys(data, 'hsvrid')
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            self.__attachLogicalVolume(lvol['lvolid'], hsvrid)
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def detachLogicalVolume(self, data):
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            lvolid = lvol['lvolid']
            root_lvolid = lvol_find_assoc_root(self.db, lvolid)
            if lvolid != root_lvolid:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            attach = self.db.attach.get_row(root_lvolid)
            if not attach:      # already dettached. nothing to do
                return 0
            if attach['eventid']:
                raise xmlrpclib.Fault(errno.EAGAIN, 'EAGAIN')
            hsvrid = attach['hsvrid']
            self.__lvol_detach(root_lvolid, lvolid, hsvrid)
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def listStorageServers(self, data):
        "get information of all storage servers."
        array = []
        try:
            ssvrid = 0
            if data.has_key('ssvrid') and data['ssvrid']:
                if not self.db.ssvrlst.get_row(data['ssvrid']):
                    # ssvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                ssvrid = data['ssvrid']

            dskspace = self.db.get_dskspace()
            dskspace_ssvr = dskspace['ssvr']

            ssvrs = self.db.ssvrlst.get_rows('ssvrid <> 0', '*')
            for ssvr in ssvrs:
                if ssvrid and ssvrid != ssvr['ssvrid']:
                    continue

                if dskspace_ssvr.has_key(ssvr['ssvrid']):
                    available, capacity = dskspace_ssvr[ssvr['ssvrid']]
                else:
                    available = 0
                    capacity = 0
                ip_addrs = self.db.ipdata.ssvr_ipdata(ssvr['ssvrid'])
                # assert len(ip_addrs) == 2
                entry = {'ssvrid': ssvr['ssvrid'], 'priority': ssvr['priority'], \
                'ip_data': ip_addrs, 'resync': self.__get_resync_load_ssvr(ssvr['ssvrid']), \
                'available': available, 'capacity': capacity}
                array.append(entry)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def listPhysicalDisks(self, data):
        "get information of all physical disks."
        array = []
        try:
            ssvrid = 0
            if data.has_key('ssvrid') and data['ssvrid']:
                if not self.db.ssvrlst.get_row(data['ssvrid']):
                    # ssvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                ssvrid = data['ssvrid']

            dskspace = self.db.get_dskspace()
            dskspace_dsk = dskspace['pdsk']

            pdsks = self.db.pdsklst.get_rows('pdskid <> 0', '*')
            for pdsk in pdsks:
                if ssvrid and ssvrid != pdsk['ssvrid']:
                    continue

                if dskspace_dsk.has_key(pdsk['pdskid']):
                    available = dskspace_dsk[pdsk['pdskid']]
                else:
                    available = 0
                entry = {'ssvrid': pdsk['ssvrid'], 'pdskid': pdsk['pdskid'], 'priority': pdsk['priority'], \
                    'local_path': pdsk['local_path'], 'srp_name': pdsk['srp_name'], \
                    'resync': self.__get_resync_load_pdsk(pdsk['pdskid']), \
                    'capacity': pdsk['capacity'], 'available': available}
                array.append(entry)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def listHeadServers(self, data):
        "get information of all head servers."
        array = []
        try:
            hsvrid = 0
            if data.has_key('hsvrid') and data['hsvrid']:
                if not self.db.hsvrlst.get_row(data['hsvrid']):
                    # hsvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                hsvrid = data['hsvrid']

            hsvrs = self.db.hsvrlst.get_rows('hsvrid <> 0', '*')
            for hsvr in hsvrs:
                if hsvrid and hsvrid != hsvr['hsvrid']:
                    continue

                ip_addrs = self.db.ipdata.hsvr_ipdata(hsvr['hsvrid'])
                entry = {'hsvrid': hsvr['hsvrid'], 'priority': hsvr['priority'], \
                    'ip_data': ip_addrs, 'resync': self.__get_resync_load_hsvr(hsvr['hsvrid'])}
                array.append(entry)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def listEvents(self, data):
        "list all event information in the system."
        array = []
# TODO
#        try:
#            transits = self.transit.get_rows('eventid <> 0', '*')
#            for transit in transits:
#                entry = {'eventid': transit['eventid'], 'target': transit['target'], \
#                'targetid': transit['id'], 'event': transit['event'], 'event_status': transit['status']}
#                array.append(entry)
#
#        except xmlrpclib.Fault:
#            raise
#        except Exception, inst:
#            logger.error(traceback.format_exc())
#            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def __scan_invalid_dexts(self, lvolid):
        lvolstruct = get_lvolstruct(self.db, lvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, vas_db.LVOLTYPE['LINEAR'])

        for lvolstruct_mirror in lvolstruct_linear['components']:
            for lvolstruct_dext in lvolstruct_mirror['components']:
                status = lvolstruct_dext['mirror_status']
                if status != vas_db.MIRROR_STATUS['INSYNC'] and status != vas_db.MIRROR_STATUS['ALLOCATED']:
                    return True
        return False

    def listLogicalVolumes(self, data):
        "list all logical volumes in the system."
        array = []
        try:
            if data.has_key('lvolid') or data.has_key('lvolname'):
                lvol = self.__get_lvol_by_id_or_name(data)
                lvols = []
                lvols.append(self.db.lvollst.get_row(lvol['lvolid']))
            else:
                lvols = self.db.lvollst.get_rows('lvolid <> 0', '*')

            for lvol in lvols:
                if lvol['deleted']:
                    continue
                lvolid = lvol['lvolid']

                attach = lvol_get_attach(self.db, lvolid)
                if attach:
                    hsvrid = attach['hsvrid']
                    bind_status = attach['status']
                    if attach['eventid']:
                        if lvolid == attach['lvolid'] or lvolid == attach['assoc_lvolid']:
                            bind_event = attach['event_type']
                        else:
                            bind_event = 0
                    else:
                        bind_event = 0
                else:
                    hsvrid = 0
                    bind_status = 0
                    bind_event = 0

                fault = self.__scan_invalid_dexts(lvolid)

                associated_to = []
                assoc = self.db.assoc.get_rows('assoc_lvolid', lvolid)
                for a in assoc:
                    associated_to.append({ \
                        'lvolid': a['lvolid'], 'type': a['type']})

                associated_from = []
                assoc = self.db.assoc.get_rows('lvolid', lvolid)
                for a in assoc:
                    associated_from.append({ \
                        'lvolid': a['assoc_lvolid'], 'type': a['type']})

                entry = {'hsvrid': hsvrid, 'lvolid': lvolid, \
                    'lvolname': lvol['lvolname'], \
                    'redundancy': lvol['redundancy'], \
                    'capacity': lvol['capacity'], \
                    'associated_to': associated_to, \
                    'associated_from': associated_from, \
                    'fault': fault, 'bind_status': bind_status, \
                    'bind_event': bind_event, 'ctime': lvol['ctime']}
                array.append(entry)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def showLogicalVolume(self, data):
        "list ditals of the volume."
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            lvolstruct = get_lvolstruct(self.db, lvol['lvolid'])

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return lvolstruct

    def __detachAllLogicalVolumesOfFailedHeadServer(self, hsvrid):
        # detach all lvols attached the failed head server.
        attaches = self.db.attach.get_rows('hsvrid', hsvrid)
        for attach in attaches:
            lvolid = attach['lvolid']
            lvol = self.db.lvollst.get_row(lvolid)
            self.db.attach.delete_rows('lvolid', lvolid)
            lvol_db_detach(self.db, lvolid)

    def __registerHeadServerTransit(self, data):
        "register an event of head server state transition." 
        hsvrid = data['targetid']
        hsvr = self.db.hsvrlst.get_row(hsvrid)
        if not hsvr:
            return 0
        if hsvr['priority'] == vas_db.ALLOC_PRIORITY['OFFLINE']:
            return 0

        try:
            self.db.begin_transaction()
            # detach all logical volumes without XML-RPC calls
            self.__detachAllLogicalVolumesOfFailedHeadServer(hsvrid)

            # change status of the head server to OFFLINE.
            self.db.hsvrlst.update_value('hsvrid', hsvrid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])
            self.db.commit()
            self.shred_cv.notifyAll()
        except Exception, inst:
            self.db.rollback()
            raise
        return 0

    def __scan_ext_status(self,pdskid):
        # scan faulty dexts then decide priority of the disk.
        dskmap_rows = self.db.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if dskmap_row['status'] == vas_db.EXT_STATUS['FAULTY']:
                return vas_db.ALLOC_PRIORITY['LOW']
        return vas_db.ALLOC_PRIORITY['HIGH']

    def __shred_offline_dexts(self, pdskid):
        # clearing offline disk extents with 0s to reuse
        dskmap_rows = self.db.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if dskmap_row['status'] != vas_db.EXT_STATUS['OFFLINE']:
                continue
            eventid = self.db.genid_event()
            self.db.shred.put_row((eventid, dskmap_row['dextid'], pdskid, \
                dskmap_row['offset'], dskmap_row['capacity'], \
                EVENT_STATUS['PENDING']))
        self.shred_cv.notifyAll()

    def __replace_dsk(self, pdskid):
        pdsklst_row = self.db.pdsklst.get_row(pdskid)
        # update pdsk first so that not to allocate dext from the pdsk
        self.db.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['FAULTY'])
        dskmap_rows = self.db.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if dskmap_row['status'] == vas_db.EXT_STATUS['BUSY']:
                self.db.replace_dext(dskmap_row['dextid'], vas_db.EXT_STATUS['FAULTY'])
            elif dskmap_row['status'] == vas_db.EXT_STATUS['FREE']:
                self.db.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])
            elif dskmap_row['status'] == vas_db.EXT_STATUS['OFFLINE']:
                self.db.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])
                self.__cancel_shred(dskmap_row['dextid'])
            else: # FAULTY or SUPER
                pass

    def __replace_ssvr(self, ssvrid):
        # replace all disk extent in a storage server
        ssvrlst_row = self.db.ssvrlst.get_row(ssvrid)
        # update ssvr first so that not to allocate dext from the ssvr
        self.db.ssvrlst.update_value('ssvrid', ssvrid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])

        pdsklst_rows = self.db.pdsklst.get_rows('ssvrid', ssvrid)
        for pdsklst_row in pdsklst_rows:
            pdskid = pdsklst_row['pdskid']
            self.db.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])
            dskmap_rows = self.db.dskmap.get_rows('pdskid', pdskid)
            for dskmap_row in dskmap_rows:
                if dskmap_row['status'] == vas_db.EXT_STATUS['BUSY']:
                    self.db.replace_dext(dskmap_row['dextid'], vas_db.EXT_STATUS['OFFLINE'])
                elif dskmap_row['status'] == vas_db.EXT_STATUS['OFFLINE']:
                    self.__cancel_shred(dskmap_row['dextid'])
                else: # FREE or FAULTY or SUPER
                    pass

    def __ssvr_failure(self, ssvrid):
        "register an event of storage server state transition." 
        ssvrlst_row = self.db.ssvrlst.get_row(ssvrid)
        if not ssvrlst_row:
            return 0
        # if priority of the storage server is already OFFLINE, nothing to do.
        if ssvrlst_row['priority'] == vas_db.ALLOC_PRIORITY['OFFLINE']:
            return 0

        try:
            self.db.begin_transaction()
            self.__replace_ssvr(ssvrid)
            self.resync_cv.notifyAll()
            self.db.commit()
        except Exception, inst:
            self.db.rollback()
            raise
        return 0

    def __pdsk_failure(self, pdskid):
        # assert in transaction
        pdsklst_row = self.db.pdsklst.get_row(pdskid)
        if not pdsklst_row:
            return 0
        # if priority of the physical disk is already OFFLINE or FAULTY, nothing to do.
        if pdsklst_row['priority'] in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
            return 0

        try:
            self.db.begin_transaction()
            self.__replace_dsk(pdskid)
            self.resync_cv.notifyAll()
            self.db.commit()
        except Exception, inst:
            self.db.rollback()
            raise
        return 0

    def notifyFailure(self, data):
        target, targetid = mand_keys(data, 'target', 'targetid')
        try:
            if target == vas_db.TARGET['HSVR']:
                return self.__registerHeadServerTransit(data)
            elif target == vas_db.TARGET['SSVR']:
                return self.__ssvr_failure(targetid)
            elif target == vas_db.TARGET['PDSK']:
                return self.__pdsk_failure(targetid)
            elif target == vas_db.TARGET['LVOL']:
                return self.__dext_failure(targetid)
            else: # invalid target
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __cancel_shred(self, dextid):
        # assert in transaction
        shred_rows = self.db.shred.get_rows('dextid', dextid)
        if not shred_rows:
            return
        assert(len(shred_rows) == 1), lineno()
        eventid = shred_rows[0]['eventid']
        status = shred_rows[0]['status']
        if status == EVENT_STATUS['PENDING']:
            logger.info("cancel pending shred: eventid(%d) dextid(%d)" % (eventid, dextid))
            self.db.shred.delete_rows('eventid', eventid)
        elif status == EVENT_STATUS['PROGRESS']:
            logger.info("mark progress shred canceled: eventid(%d) dextid(%d)" % \
                (eventid, dextid))
            self.db.shred.update_value('eventid', eventid, 'status', EVENT_STATUS['CANCELED'])

    def __dext_failure(self, dextid):
        dskmap = self.db.dskmap.get_row(dextid)
        if not dskmap:
            logger.error("__dext_faulure: dextid (%d) not exist" % (dextid))
            raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

        # if priority of the disk is already FAULTY, nothing to do.
        pdskid = dskmap['pdskid']
        pdsklst_row = self.db.pdsklst.get_row(pdskid)
        # check pdsk first (if pdsk is faulty, dskmap status not changed)
        if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['FAULTY']:
            return 0

        # if status of the disk extent is already FAULTY, nothing to do.
        if dskmap['status'] == vas_db.EXT_STATUS['FAULTY']:
            return 0

        lvolmap = self.db.lvolmap.get_row(dextid)
        attach = lvol_get_attach(self.db, lvolmap['toplvolid'])
        if not attach:
            # already detached
            return 0
        if lvolmap['mirror_status'] == vas_db.MIRROR_STATUS['SPARE']:
            # already resync scheduled
            return 0
        assert(lvolmap['mirror_status'] == vas_db.MIRROR_STATUS['INSYNC']), lineno()

        self.db.begin_transaction()
        try:
            self.db.lvolmap.update_value('lvolid', dextid, 'mirror_status', vas_db.MIRROR_STATUS['SPARE'])
            eventid = self.db.genid_event()
            self.db.resync.put_row((eventid, lvolmap['superlvolid'], dextid, dextid, EVENT_STATUS['PENDING']))
            self.resync_cv.notifyAll()
            self.db.commit()
        except Exception, inst:
            self.db.rollback()
            raise
        return 0

    def notifyBadBlocks(self, data):
        pdskid, blocks = mand_keys(data, 'pdskid', 'blocks')
        try:
            # if priority of the disk is already FAULTY, nothing to do.
            pdsklst_row = self.db.pdsklst.get_row(pdskid)
            if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['FAULTY']:
                return 0

            dskmap_rows=[]
            for block in blocks:
                dskmap_row = self.db.dskmap.get_extent(pdskid, block)
                if not dskmap_row:
                    # disk extent is not found on the data base
                    raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                # if status of the disk extent is already FAULTY, nothing to do.
                if dskmap_row['status'] == vas_db.EXT_STATUS['FAULTY']:
                    logger.debug("notifyBadBlocks: the block(%d@pdsk-%08x) is already in FAULTY status. the block is skipped." % (block, pdskid))
                    continue
                dskmap_rows.append(dskmap_row)    
            
            if len(dskmap_rows) == 0:
                return 0

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db.begin_transaction()
            for dskmap_row in dskmap_rows:
                if dskmap_row['status'] == vas_db.EXT_STATUS['BUSY']:
                    self.db.replace_dext(dskmap_row['dextid'], vas_db.EXT_STATUS['FAULTY'])
                elif dskmap_row['status'] == vas_db.EXT_STATUS['FREE']:
                    self.db.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])
                elif dskmap_row['status'] == vas_db.EXT_STATUS['OFFLINE']:
                    self.db.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])
                    # TODO cancel shred
                else: # SUPER
                    pass
            if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['HIGH']:
                # HIGH --> HIGH/LOW
                self.db.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['LOW'])
            self.resync_cv.notifyAll()
            self.db.commit()
        except xmlrpclib.Fault:
            self.db.rollback()
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __check_ip_data(self, data):
        ip_addr_re = re.compile("^\d+\.\d+\.\d+\.\d+$")
        if not data.has_key('ip_data'):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        if len(data['ip_data']) == 0:
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        for ip_data in data['ip_data']:
            if not ip_addr_re.match(ip_data):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

    def registerHeadServer(self, data):
        "register a head server." 
        try:
            self.__check_ip_data(data)
            hsvrid = self.db.ipdata.hsvr_match(data['ip_data'])
            if hsvrid != 0:
                # the head server is already registered
                hsvr = self.db.hsvrlst.get_row(hsvrid)
                if hsvr['priority'] != vas_db.ALLOC_PRIORITY['OFFLINE']:
                    raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db.begin_transaction()
            if hsvrid != 0:
                # OFFLINE --> HIGH
                self.db.hsvrlst.update_value('hsvrid', hsvrid, 'priority', vas_db.ALLOC_PRIORITY['HIGH'])
            else:
                hsvrid = self.db.genid_hsvr()
                self.db.hsvrlst.put_row((hsvrid, vas_db.ALLOC_PRIORITY['HIGH'], 0))
                self.db.ipdata.put_hsvr_ipdata(hsvrid, data['ip_data'])
            self.db.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return hsvrid

    def registerStorageServer(self, data):
        "register a storage server." 
        try:
            self.__check_ip_data(data)
            ssvrid = self.db.ipdata.ssvr_match(data['ip_data'])
            if ssvrid != 0:
                # the storage server is already registered
                ssvr = self.db.ssvrlst.get_row(ssvrid)
                if ssvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['HALT']):
                    raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db.begin_transaction()
            if ssvrid != 0:
                # OFFLINE --> HIGH
                self.db.ssvrlst.update_value('ssvrid', ssvrid, 'priority', vas_db.ALLOC_PRIORITY['HIGH'])
            else:
                ssvrid = self.db.genid_ssvr()
                self.db.ssvrlst.put_row((ssvrid, vas_db.ALLOC_PRIORITY['HIGH'], 0))
                self.db.ipdata.put_ssvr_ipdata(ssvrid, data['ip_data'])
            self.db.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return ssvrid

    def registerPhysicalDisk(self, data):
        "register a physical disk." 
        try:
            if not data.has_key('ssvrid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if not data.has_key('local_path'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if not data.has_key('capacity'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            ssvr = self.db.ssvrlst.get_row(data['ssvrid'])
            if not ssvr:
                # invalid ssvrid
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if ssvr['priority'] != vas_db.ALLOC_PRIORITY['HIGH']:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            pdsk_is_offline = False
            pdskid = 0
            dsks = self.db.pdsklst.get_rows('ssvrid',data['ssvrid'])
            for dsk in dsks:
                if dsk['local_path'] == data['local_path']:
                    # the disk is already registered
                    if dsk['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['HALT']):
                        # EEXIST error makes storage server shutdown
                        raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
                    pdsk_is_offline = True
                    pdskid = dsk['pdskid']
                    break
            if pdskid == 0:
                self.db.begin_transaction()
                try:
                    pdskid = self.db.genid_pdsk()
                    superid = self.db.genid_lvol()
                except:
                    self.db.rollback()
                    raise
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            if pdsk_is_offline:
                self.db.begin_transaction()
                # OFFLINE --> HIGH/LOW
                self.db.pdsklst.update_value('pdskid', pdskid, 'priority', self.__scan_ext_status(pdskid))

                # clearing offline disk extents with 0s to reuse
                self.__shred_offline_dexts(pdskid)
            else:
                # inTransaction
                # NONE --> HIGH

                # data['capacity'] is size in sector_str
                try:
                    size_in_sector = int(data['capacity'], 10)
                except:
                    raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                if size_in_sector <= 0:
                    raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                size_in_giga, odd = stovb(size_in_sector)

                # brand-new disk.
                # clearing of an entire physical disk and divide the sectors onto a free disk extent for data and a super block for meta data.

                super_sectors = odd
                if super_sectors == 0:
                    size_in_giga -= 1
                    super_sectors = vbtos(1)

                ip_addrs = self.db.ipdata.ssvr_ipdata(data['ssvrid'])
                # assert len(ip_addrs) == 2
                self.db.pdsklst.put_row((data['ssvrid'], pdskid, size_in_giga, \
                   data['srp_name'], data['local_path'], vas_db.ALLOC_PRIORITY['HIGH']))
                offset = 0
                unit = max(EXTENTSIZE)
                while offset < size_in_giga:
                    lvolid = self.db.genid_lvol()
                    if not lvolid:
                        raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
                    if size_in_giga - offset >= unit:
                        extent_size = unit
                    else:
                        extent_size = size_in_giga - offset
                    self.db.dskmap.put_row((pdskid, lvolid, offset, \
                        extent_size, vas_db.EXT_STATUS['OFFLINE']))
                    eventid = self.db.genid_event()
                    self.db.shred.put_row((eventid, lvolid, pdskid, offset, \
                        extent_size, EVENT_STATUS['PENDING']))
                    offset += extent_size
                self.db.dskmap.put_row((pdskid, superid, size_in_giga, \
                    super_sectors, vas_db.EXT_STATUS['SUPER']))

            self.db.commit()
            self.shred_cv.notifyAll()
        except xmlrpclib.Fault:
            # __shredDiskExtent error case
            self.db.rollback()
            raise
        except Exception, inst:
            self.db.rollback()
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return pdskid

    def __dextid_to_ssvrid(self, lvolid):
        dskmap_row = self.db.dskmap.get_row(lvolid)
        pdsklst_row = self.db.pdsklst.get_row(dskmap_row['pdskid'])
        return pdsklst_row['ssvrid']

    def __dextid_to_pdskid(self, lvolid):
        dskmap_row = self.db.dskmap.get_row(lvolid)
        return dskmap_row['pdskid']

    def __dextid_to_hsvrid(self, lvolid):
        dext = self.db.lvolmap.get_row(lvolid)
        attach = lvol_get_attach(self.db, dext['toplvolid'])
        assert(attach), lineno()
        return attach['hsvrid']

    def __mirrorid_to_hsvrid(self, lvolid):
        lvolmap_row = self.db.lvolmap.get_row(lvolid)
        attach = lvol_get_attach(self.db, lvolmap_row['toplvolid'])
        assert(attach), lineno()
        return attach['hsvrid']

    def deletePhysicalDisk(self, data):
        pdskid = mand_keys(data, 'pdskid')
        try:
            if not self.db.pdsklst.get_row(pdskid):
                # pdsk entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # check physical disk is offline
            try:
                self.__assert_dsk_offline(pdskid)
            except Exception, inst:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            
            # delete a entry from db
            try:
                self.db.begin_transaction()
                self.__delete_offline_disk(pdskid)
                self.db.commit()    
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def deleteHeadServer(self, data):
        hsvrid = mand_keys(data, 'hsvrid')
        try:
            hsvr = self.db.hsvrlst.get_row(hsvrid)
            if not hsvr:
                # hsvrlst entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # chech the head server is offline
            if hsvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            attach = self.db.attach.get_rows('hsvrid', hsvrid)
            if attach:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            # delete entries from DB
            try:
                self.db.begin_transaction()
                self.db.hsvrlst.delete_rows('hsvrid', hsvrid)
                self.db.ipdata.delete_hsvr(hsvrid)
                self.db.commit()            
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def deleteStorageServer(self, data):
        ssvrid = mand_keys(data, 'ssvrid')
        try:
            ssvr = self.db.ssvrlst.get_row(ssvrid)
            if not ssvr:
                # ssvrlst entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # chech the storage server is offline
            if ssvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            # delete entries from DB
            try:
                self.db.begin_transaction()
                dsks = self.db.pdsklst.get_rows('ssvrid', ssvrid)
                for dsk in dsks:
                    self.__delete_offline_disk(dsk['pdskid'])
                self.db.ssvrlst.delete_rows('ssvrid', ssvrid)
                self.db.ipdata.delete_ssvr(ssvrid)
                self.db.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __offlineAll(self):
        for hsvr in self.db.hsvrlst.get_rows('hsvrid <> 0', '*'):
            if hsvr['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.db.hsvrlst.update_value('hsvrid', hsvr['hsvrid'], 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])
        for ssvr in self.db.ssvrlst.get_rows('ssvrid <> 0', '*'):
            if ssvr['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.db.ssvrlst.update_value('ssvrid', ssvr['ssvrid'], 'priority', vas_db.ALLOC_PRIORITY['HALT'])
        for pdsk in self.db.pdsklst.get_rows('pdskid <> 0', '*'):
            if pdsk['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.db.pdsklst.update_value('pdskid', pdsk['pdskid'], 'priority', vas_db.ALLOC_PRIORITY['HALT'])

    def shutdownAll(self, data):
        try:
            if self.db.attach.rowcount() > 0:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db.begin_transaction()
            self.__offlineAll()
            self.db.commit()

        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __send_request(self, ip_addrs, port, method, data):
        try:
            return self.__send_request_raw(ip_addrs, port, method, data)
        except xmlrpclib.Fault:
            raise
        except SMExceptionSocketTimeout:
            xmlrpclib.Fault(errno.ETIMEDOUT, 'ETIMEDOUT')
        except:
            raise xmlrpclib.Fault(500, 'Internal Server Error')

    def __send_request_raw(self, ip_addrs, port, method, data):
        logger.debug("__send_request %s %s %s %s" % \
            (ip_addrs, port, method, data))
        for ip in ip_addrs:
            try:
                agent = xmlrpclib.ServerProxy("http://%s:%s" % (ip, port))
                func = getattr(agent, method)
                res = func(data)
                logger.debug("__send_request: %s respons: %s" % (ip, res))
                return res
            except socket.timeout, inst:
                # timeout
                logger.debug("__send_request: %s timeout: %s", (ip, inst))
                raise SMExceptionSocketTimeout
            except xmlrpclib.Fault, inst:
                # Exceptions on remote side
                logger.error( \
                    "__send_request: %s Exceptions on remote side: %s", \
                    (ip, inst))
                raise
            except Exception, inst:
                # try other link
                logger.debug("__send_request %s failed %s" % (ip, inst))
                continue
        # both link down or server down
        logger.error("__send_request: both link down or server down")
        raise SMExceptionBothLinkDown()

def usage():
    print 'usage: %s [-h|--host] [--help] ' % sys.argv[0]

def sighandler(signum, frame):
    global storage_manager_object
    try:
        storage_manager_object.disconnect_db()
    except Exception, inst:
        logger.error(traceback.format_exc())
        sys.exit(1)
    sys.exit(0)

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():
    global main_lock, storage_manager_object
    try:
        opts, _args = getopt.getopt(sys.argv[1:], "h:", ["host=","help"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    host='0.0.0.0'

    for o, a in opts:
        if o == "--help":
            usage()
            sys.exit(2)
        elif o in ("-h", "--host"):
            host = a

    signal.signal( signal.SIGINT, sighandler )
    signal.signal( signal.SIGTERM, sighandler )

    main_lock = threading.Lock()

    storage_manager_object = StorageManager()

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    Tserver.allow_reuse_address=True
    server = Tserver((host, port_storage_manager))
    server.register_instance(storage_manager_object)

    server.register_introspection_functions()

    #Go into the main listener loop
    print "Listening on port %s" % port_storage_manager
    server.serve_forever()

if __name__ == "__main__":
    main()
