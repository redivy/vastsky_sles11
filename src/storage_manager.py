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

__version__ = '$Id: storage_manager.py 121 2010-07-26 03:17:48Z yamamoto2 $'

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
import threading    # RequestWorker
from SocketServer import ThreadingMixIn
from types import *
from vas_conf import *
from vas_subr import lineno, gtos, stovb, vbtos, executecommand, get_lvolstruct_of_lvoltype, getRoundUpCapacity, check_lvolname

class StorageManager:
    def __init__(self):
        pass

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
        if not os.path.exists(DB_EVENTS):
            raise Exception, "%s not exists." % (DB_EVENTS)

        # for begin/commit/rollback only
        self.db_components = vas_db.Db_base()
        self.db_components.connect(DB_COMPONENTS)
        self.db_events = vas_db.Db_base()
        self.db_events.connect(DB_EVENTS)

        # tables
        self.transit = vas_db.Db_transit()
        self.resync = vas_db.Db_resync()
        self.shred = vas_db.Db_shred()
        self.lvollst = vas_db.Db_lvollst()
        self.ssvrlst = vas_db.Db_ssvrlst()
        self.hsvrlst = vas_db.Db_hsvrlst()
        self.pdsklst = vas_db.Db_pdsklst()
        self.dskmap = vas_db.Db_dskmap()
        self.lvolmap = vas_db.Db_lvolmap()
        self.genid_ssvr = vas_db.Db_genid_ssvr()
        self.genid_hsvr = vas_db.Db_genid_hsvr()
        self.genid_dsk = vas_db.Db_genid_dsk()
        self.genid_event = vas_db.Db_genid_event()
        self.genid_lvol = vas_db.Db_genid_lvol()
        self.lv = vas_db.LogicalVolume() 

    def disconnect_db(self):
        self.db_components.disconnect()
        self.db_events.disconnect()

        self.transit.disconnect()
        self.resync.disconnect()
        self.shred.disconnect()
        self.lvollst.disconnect()
        self.ssvrlst.disconnect()
        self.hsvrlst.disconnect()
        self.pdsklst.disconnect()
        self.dskmap.disconnect()
        self.lvolmap.disconnect()
        self.genid_ssvr.disconnect()
        self.genid_hsvr.disconnect()
        self.genid_dsk.disconnect()
        self.genid_event.disconnect()
        self.genid_lvol.disconnect()
        self.lv.disconnect()

    def __get_resync_load(self, hsvrid, dextid):
        dskmap_row = self.dskmap.get_row(dextid)
        hsvrlst_row = self.hsvrlst.get_row(hsvrid)
        pdsklst_row = self.pdsklst.get_row(dskmap_row['pdskid'])
        ssvrlst_row = self.ssvrlst.get_row(pdsklst_row['ssvrid'])
        return (hsvrlst_row['resync'], ssvrlst_row['resync'], pdsklst_row['resync'])

    def __inc_resync_load(self, hsvrid, dextid):
        hsvr_load, ssvr_load, dsk_load = self.__get_resync_load(hsvrid, dextid)
        dskmap_row = self.dskmap.get_row(dextid)
        pdsklst_row = self.pdsklst.get_row(dskmap_row['pdskid'])

        self.hsvrlst.update_value('hsvrid', hsvrid, 'resync', hsvr_load + 1)
        self.ssvrlst.update_value('ssvrid', pdsklst_row['ssvrid'], 'resync', ssvr_load + 1)
        self.pdsklst.update_value('pdskid', dskmap_row['pdskid'], 'resync', dsk_load + 1)

    def __dec_resync_load(self, hsvrid, dextid):
        hsvr_load, ssvr_load, dsk_load = self.__get_resync_load(hsvrid, dextid)
        assert (hsvr_load > 0), lineno()
        assert (ssvr_load > 0), lineno()
        assert (dsk_load > 0), lineno()

        dskmap_row = self.dskmap.get_row(dextid)
        pdsklst_row = self.pdsklst.get_row(dskmap_row['pdskid'])

        self.hsvrlst.update_value('hsvrid', hsvrid, 'resync', hsvr_load - 1)
        self.ssvrlst.update_value('ssvrid', pdsklst_row['ssvrid'], 'resync', ssvr_load - 1)
        self.pdsklst.update_value('pdskid', dskmap_row['pdskid'], 'resync', dsk_load - 1)

    def __progress_resync(self, resync_row):
        # bind new spare to mirror on the DB
        self.lvolmap.update_value('lvolid', resync_row['lvolid_add'], 'status', vas_db.BIND_STATUS['BOUND'])

        # update resync mirror event status. PENDING to PROGRESS 
        self.transit.update_value('eventid', resync_row['eventid'], 'status', vas_db.EVENT_STATUS['PROGRESS'])

        # increment resync load value
        hsvrid = self.__mirrorid_to_hsvrid(resync_row['mirrorid'])
        assert(hsvrid), lineno()

        self.__inc_resync_load(hsvrid, resync_row['lvolid_add'])

    def __close_resync(self, resync_row):
        eventid = resync_row['eventid']

        # delete finished resync task
        self.resync.delete_rows('eventid', eventid)

        # update resync mirror event status. PROGRESS to CLOSED
        #self.transit.update_value('eventid', eventid, 'status', vas_db.EVENT_STATUS['CLOSED'])
        self.transit.delete_rows('eventid', eventid)
        self.genid_event.recover(eventid)

        # decrement resync load value
        hsvrid = self.__mirrorid_to_hsvrid(resync_row['mirrorid'])
        self.__dec_resync_load(hsvrid, resync_row['lvolid_add'])

    def __retrieve_pending_resyncs(self, pdskid, hsvrid):
        # retrive pending resync requests on a failed disk or head server
        canceled_resyncs = []

        def cancel_resync(task):
            resync_row = self.resync.get_row(task['eventid'])
            logger.debug( "__retrieve_pending_resyncs: resync canceled. eventid: %d dextid: %d" % (task['eventid'], task['id']))
            if task['status'] != vas_db.EVENT_STATUS['PROGRESS']:
                canceled_resyncs.append(resync_row)
                self.__progress_resync(resync_row)
            self.__close_resync(resync_row)

        resyncs_to_do = self.transit.get_resyncs(vas_db.EVENT_STATUS['PENDING'])
        resyncs_to_do += self.transit.get_resyncs(vas_db.EVENT_STATUS['PROGRESS'])
        if len(resyncs_to_do) == 0:
            return None
        if pdskid:
            dextids_evacuate = self.dskmap.get_dextids_evacuate(pdskid)
            assert(len(dextids_evacuate) > 0), lineno()

            for task in resyncs_to_do:
                if task['id'] in dextids_evacuate:
                    # finish the resync task without real processing
                    cancel_resync(task)
                else:
                    logger.debug( "__retrieve_pending_resyncs: resync not canceled. eventid: %d dextid: %d" % (task['eventid'], task['id']))
        else:
            for task in resyncs_to_do:
                resync_row = self.resync.get_row(task['eventid'])
                if self.__mirrorid_to_hsvrid(resync_row['mirrorid']) == hsvrid:
                    cancel_resync(task)
                else:
                    logger.debug( "__retrieve_pending_resyncs: resync not canceled. eventid: %d dextid: %d" % (task['eventid'], task['id']))

        return canceled_resyncs

    def __retrieve_pending_shreds(self, pdskid, ssvrid):
        # retrive pending shred requests on a failed disk or storage server

        def cancel_shred(task):
            # delete shred task to cancel execution
            self.shred.delete_rows('eventid', task['eventid'])
            self.transit.delete_rows('eventid', task['eventid'])
            self.genid_event.recover(task['eventid'])

            logger.debug( "__retrieve_pending_shreds: shred canceled. eventid: %d dextid: %d" % (task['eventid'], task['id']))

        shreds_to_do = self.transit.get_shreds(vas_db.EVENT_STATUS['PENDING'])
        shreds_to_do += self.transit.get_shreds(vas_db.EVENT_STATUS['PROGRESS'])
        if len(shreds_to_do) == 0:
            return
        if pdskid:
            assert(ssvrid == 0), lineno()
            dextids_evacuate = self.dskmap.get_dextids_evacuate(pdskid)

            for task in shreds_to_do:
                if task['id'] in dextids_evacuate:
                    cancel_shred(task)
        else:
            for task in shreds_to_do:
                if self.__dextid_to_ssvrid(task['id']) == ssvrid:
                    cancel_shred(task)

    def __recovering_mirror(self, pdskid):
        # retrive pending shred/resync requests on a failed disk
        canceled_resyncs = self.__retrieve_pending_resyncs(pdskid, 0)
        self.__retrieve_pending_shreds(pdskid, 0)

        data = self.lv.replace_disk(pdskid)
        for hsvrid in data.keys():
            mirrors = data[hsvrid]

            # issue resync mirror events with PENDING status

            for mdinfo in mirrors:
                add = mdinfo['add']
                rm = mdinfo['remove']
                eventid = self.genid_event.genid()
                if not eventid:
                    # can not get new eventid
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
                self.transit.put_row((eventid, vas_db.TARGET['LVOL'], add['lvolid'], vas_db.EVENT['RESYNC'], vas_db.EVENT_STATUS['PENDING']))
                logger.debug("__recovering_mirror: event: %d %s %d %s %s" % (eventid, 'LVOL', add['lvolid'], 'RESYNC', 'PENDING'))
                mdinfo['eventid'] = eventid
                self.resync.put_row((eventid, mdinfo['id'], add['lvolid'], rm['lvolid'], add['ssvrid'], rm['ssvrid'], add['iscsi_path'][0], rm['iscsi_path'][0], add['iscsi_path'][1], rm['iscsi_path'][1], add['capacity'], add['offset']))

            # merge canceled resync tasks to each correspondind new tasks
            if canceled_resyncs:
                for resync_row in canceled_resyncs:
                    self.resync.merge_row(resync_row)

        # send a resync request to hsvr_agent
        self.__recovering_mirror_post(MAX_RESYNC_TASKS)

    def __recovering_mirror_post(self, count):
        assert(count > 0), lineno()

        # limit number of resync tasks run simulataneously
        outstanding_resyncs = self.transit.count_progress_resyncs()
        if outstanding_resyncs >= count:
            return
        count -= outstanding_resyncs

        # choose resync tasks and build requests for an hsvr_agent
        resyncs_to_do = self.transit.get_resyncs(vas_db.EVENT_STATUS['PENDING'])
        if len(resyncs_to_do) < count:
            count = len(resyncs_to_do)
        if len(resyncs_to_do) == 0:
            return
        
        # count valid copy for each dexts
        priority_points = {}
        pdskids = {}
        disk_semaphore = {}
        for task in resyncs_to_do:
            dextid = task['id']
            hsvrid = self.__dextid_to_hsvrid(dextid)
            assert(hsvrid), lineno()
            ssvrid = self.__dextid_to_ssvrid(dextid)
            pdskid = self.__dextid_to_pdskid(dextid)

            # do not put more load on disks under resync
            if self.pdsklst.get_resync_load(pdskid):
                continue

            eventid = task['eventid']
            priority_points[eventid] = self.lvolmap.count_valid_copy(dextid) * 10
            priority_points[eventid] += self.hsvrlst.get_resync_load(hsvrid) * 1
            priority_points[eventid] += self.ssvrlst.get_resync_load(ssvrid) * 1

            pdskids[eventid] = pdskid
            disk_semaphore[pdskid] = 0

        # build and send resync request for hsvr_agent
        for eventid, _priority_point in sorted(priority_points.items(), key=lambda (k, v) : (v, k)):
            if count <= 0:
                break
            if disk_semaphore[pdskids[eventid]]:
                # do not execute more than one resync on a disk
                continue
            else:
                count -= 1
                disk_semaphore[pdskids[eventid]] = 1

            resync_row = self.resync.get_row(eventid)

            lvolspec_add = { 'pdskid': 0, 'offset': resync_row['offset'], 'ssvrid': resync_row['ssvrid_add'], \
            'status': 0, 'iscsi_path': (resync_row['iscsi_path_1_add'], resync_row['iscsi_path_2_add']) }

            lvolstruct_add = { 'lvolid': resync_row['lvolid_add'], 'lvoltype': vas_db.LVOLTYPE['DEXT'], \
            'capacity': resync_row['capacity'], 'bind_status': 0, 'lvolspec': lvolspec_add }

            lvolspec_remove = { 'pdskid': 0, 'offset': 0, 'ssvrid': resync_row['ssvrid_rm'], \
            'status': 0, 'iscsi_path': (resync_row['iscsi_path_1_rm'], resync_row['iscsi_path_2_rm']) }

            lvolstruct_remove = { 'lvolid': resync_row['lvolid_rm'], 'lvoltype': vas_db.LVOLTYPE['DEXT'], \
            'capacity': 0, 'bind_status': 0, 'lvolspec': lvolspec_remove }

            lvolstruct = { 'lvolid': resync_row['mirrorid'], 'lvoltype': vas_db.LVOLTYPE['MIRROR'], \
            'capacity': 0, 'bind_status': 0, 'lvolspec': { 'add': lvolstruct_add, 'remove': lvolstruct_remove } }

            subargs = { 'ver': XMLRPC_VERSION, 'lvolstruct': lvolstruct }

            hsvrid = self.__mirrorid_to_hsvrid(resync_row['mirrorid'])
            hsvrinfo = self.hsvrlst.get_row(hsvrid)
            ip_addrs = (hsvrinfo['ip_data_1'], hsvrinfo['ip_data_2'])
            self.__call_request(ip_addrs, port_hsvr_agent, "replaceMirrorDisk", subargs)
            self.__progress_resync(resync_row)

    def __assert_dsk_offline(self, pdskid):
        dsk = self.pdsklst.get_row(pdskid)
        assert(dsk), lineno()
        if dsk['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'] ,vas_db.ALLOC_PRIORITY['FAULTY']):
            raise Exception, "__assert_dsk_offline: physical disk(pdsk-%08x) is not offline" % pdskid

    def __assert_dext_offline(self, lvolid):
        dskmp = self.dskmap.get_row(lvolid)
        assert(dskmp), lineno()
        if dskmp['status'] not in (vas_db.EXT_STATUS['OFFLINE'], vas_db.EXT_STATUS['FAULTY'], vas_db.EXT_STATUS['FREE'], vas_db.EXT_STATUS['SUPER']):
            raise Exception, "__assert_dext_offline: dext(dext-%08x) is not offline" % lvolid
        lvmp = self.lvolmap.get_row(lvolid)
        if lvmp:
            raise Exception, "__assert_dext_offline: dext(dext-%08x) is allocated" % lvolid

    def __delete_offline_disk(self, pdskid):
        self.__assert_dsk_offline(pdskid)

        dskmps = self.dskmap.get_rows('pdskid', pdskid)
        for dskmp in dskmps:
            self.__assert_dext_offline(dskmp['dextid'])

        self.pdsklst.delete_rows('pdskid', pdskid)
        self.dskmap.delete_rows('pdskid', pdskid)

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

            if not data.has_key('capacity') or type(data['capacity']) != IntType:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            capacity = getRoundUpCapacity(data['capacity'])
            if capacity < 1 or capacity > MAX_LENGTH :
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            if self.lvollst.get_rows('lvolname', lvolname):
                # volume already exists
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')

            dminfo = {}

            # create logical volume in DB
            try:
                self.db_components.begin_transaction()
                lvolid = self.lv.create(lvolname, redundancy, capacity, dminfo)
                self.db_components.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_components.rollback()
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return lvolid

    def __shredDiskExtent(self, lvolids):
        # issue shred dext events with PENDING status
        for lvolid in lvolids:
            dskmp = self.dskmap.get_row(lvolid)
            dsk = self.pdsklst.get_row(dskmp['pdskid'])

            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new eventid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            self.transit.put_row((eventid, vas_db.TARGET['LVOL'], lvolid, vas_db.EVENT['SHRED'], vas_db.EVENT_STATUS['PENDING']))
            logger.debug("__shredDiskExtent: event: %d %s %d %s %s" % (eventid, 'LVOL', lvolid, 'SHRED', 'PENDING'))
            self.shred.put_row((eventid, lvolid, dskmp['offset'], dskmp['capacity'], dsk['pdskid']))

        # send a shred request to ssvr_agent
        self.__shredDiskExtent_post(MAX_SHRED_TASKS)
        return 0

    def __shredDiskExtent_post(self, count):
        # choose shred tasks and build requests for an ssvr_agent
        assert(count > 0), lineno()

        # limit number of shred tasks run simulataneously
        outstanding_shreds = self.transit.count_progress_shreds()
        if outstanding_shreds >= count:
            return
        count -= outstanding_shreds

        shreds_to_do = self.transit.get_shreds(vas_db.EVENT_STATUS['PENDING'])
        if len(shreds_to_do) < count:
            count = len(shreds_to_do)
        if len(shreds_to_do) == 0:
            return
        
        # count running shreds per disks
        shred_loads = {}
        shreds_progress = self.transit.get_shreds(vas_db.EVENT_STATUS['PROGRESS'])
        for task_progress in shreds_progress:
            dextid_progress = task_progress['id']
            pdskid_progress = self.__dextid_to_pdskid(dextid_progress)
            if shred_loads.has_key(pdskid_progress):
                shred_loads[pdskid_progress] += 1
            else:
                shred_loads[pdskid_progress] = 1

        # count valid copy for each dexts
        priority_points = {}
        for task in shreds_to_do:
            dextid = task['id']
            ssvrid = self.__dextid_to_ssvrid(dextid)
            pdskid = self.__dextid_to_pdskid(dextid)

            # do not put more load on disks under resync
            if self.pdsklst.get_resync_load(pdskid):
                continue

            # do not put more load on disks under shred
            if shred_loads.has_key(pdskid):
                if shred_loads[pdskid] >= MAX_SHRED_TASKS_PER_DISK:
                    logger.debug( "__shredDiskExtent_post eventid: %d dextid: %d ssvrid: %d pdskid: %d shred_load: %d" % (task['eventid'], dextid, ssvrid, pdskid, shred_loads[pdskid]))
                    continue

            if shred_loads.has_key(pdskid):
                shred_loads[pdskid] += 1
            else:
                shred_loads[pdskid] = 1

            eventid = task['eventid']
            priority_points[eventid] = self.ssvrlst.get_resync_load(ssvrid)

        # build and send resync request for ssvr_agent
        for eventid, _priority_point in sorted(priority_points.items(), key=lambda (k, v) : (v, k))[:count]:
            shred_row = self.shred.get_row(eventid)

            dext = {'ver': XMLRPC_VERSION, 'dextid': shred_row['lvolid'], \
            'offset': shred_row['offset'], 'capacity': shred_row['capacity'], \
            'pdskid': shred_row['pdskid']}

            logger.debug("call registerShredRequest. %s" % dext)

            ssvrid = self.__dextid_to_ssvrid(dext['dextid'])

            ssvrinfo = self.ssvrlst.get_row(ssvrid)
            self.__call_request((ssvrinfo['ip_data_1'], ssvrinfo['ip_data_2']), port_ssvr_agent, "registerShredRequest", dext)
            # update shred dext event status. PENDING to PROGRESS 
            self.transit.update_value('eventid', eventid, 'status', vas_db.EVENT_STATUS['PROGRESS'])

    def deleteLogicalVolume(self, data):
        try:
            lvol = self.__get_lvol_by_id_or_name(data)

            try:
                self.db_components.begin_transaction()
                offlined_dexts = self.lv.remove(lvol['lvolid'])
                self.db_components.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_components.rollback()
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            try:
                self.db_events.begin_transaction()
                if len(offlined_dexts) != 0:
                    self.__shredDiskExtent(offlined_dexts)
                self.db_events.commit()
            except xmlrpclib.Fault:
                # __shredDiskExtent error case
                self.db_events.rollback()
                raise
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_events.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __attachLogicalVolume(self, data):
        global main_lock
        lvolmap = self.lvolmap.get_row(data['lvolid'])
        if lvolmap['status'] not in (vas_db.BIND_STATUS['UNBOUND'], vas_db.BIND_STATUS['ALLOCATED']):
            raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

        hsvrlst = self.hsvrlst.get_row(data['hsvrid'])
        if hsvrlst['priority'] in (vas_db.ALLOC_PRIORITY['EVACUATE'], vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

        lvolstruct = self.lv.get_lvolstruct(data['lvolid'])

        # save_status/*/* --> BINDING/*/*
        self.db_components.begin_transaction()
        save_status = self.lv.binding(data['lvolid'], data['hsvrid'])
        self.db_components.commit()

        main_lock.release()
        try:
            subargs = {'ver': XMLRPC_VERSION, 'lvolstruct': lvolstruct}
            self.__send_request((hsvrlst['ip_data_1'], hsvrlst['ip_data_2']), port_hsvr_agent, "attachLogicalVolume", subargs)
        except Exception, inst:
            main_lock.acquire()

            # BINDING/*/* --> save_status/*/*
            self.db_components.begin_transaction()
            self.lvolmap.update_value('lvolid', data['lvolid'], 'status', save_status)
            self.lvollst.update_value('lvolid', data['lvolid'], 'hsvrid', 0)
            self.db_components.commit()
            raise
        main_lock.acquire()

        try:
            # BINDING/ALLOCATED/UNBOUND --> BOUND/BOUND/BOUND
            self.db_components.begin_transaction()
            self.lv.bind(data['lvolid'], data['hsvrid'])
            self.db_components.commit()
        except Exception, inst:
            self.db_components.rollback()
            raise


    def __detachLogicalVolume(self, data):
        global main_lock
        lvolmap = self.lvolmap.get_row(data['lvolid'])
        if lvolmap['status'] in (vas_db.BIND_STATUS['BINDING'], vas_db.BIND_STATUS['UNBINDING']):
            raise xmlrpclib.Fault(errno.EAGAIN, 'EAGAIN')

        if lvolmap['status'] != vas_db.BIND_STATUS['BOUND']:
            return

        if self.__scan_invalid_dexts(data['lvolid']):
            # the volume is in resync
            raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

        lvolstruct = self.lv.get_lvolstruct(data['lvolid'])
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, vas_db.LVOLTYPE['LINEAR'])
        hsvr = self.hsvrlst.get_row(lvolstruct_linear['lvolspec']['hsvrid'])
        ip_addrs = (hsvr['ip_data_1'], hsvr['ip_data_2'])

        # save_status/*/* --> UNBINDING/*/*
        self.db_components.begin_transaction()
        save_status = self.lv.unbinding(data['lvolid'])
        self.db_components.commit()

        main_lock.release()
        try:
            subargs = {'ver': XMLRPC_VERSION, 'lvolstruct': lvolstruct}
            self.__send_request(ip_addrs, port_hsvr_agent, "detachLogicalVolume", subargs)
        except Exception, inst:
            main_lock.acquire()

            # UNBINDING/*/* --> save_status/*/*
            self.db_components.begin_transaction()
            self.lvolmap.update_value('lvolid', data['lvolid'], 'status', save_status)
            self.db_components.commit()
            raise
        main_lock.acquire()

        # UNBINDING/*/* --> UNBOUND/UNBOUND/UNBOUND
        self.db_components.begin_transaction()
        self.lv.unbound(data['lvolid'])
        self.db_components.commit()

    def __get_lvol_by_id_or_name(self, data):
        lvol = None
        if data.has_key('lvolid') and data['lvolid']:
            lvol = self.lvollst.get_row(data['lvolid'])
        elif data.has_key('lvolname') and data['lvolname']:
            lvols = self.lvollst.get_rows('lvolname', data['lvolname'])
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
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            data['lvolid'] = lvol['lvolid']

            hsvr = None
            if data.has_key('hsvrid') and data['hsvrid']:
                hsvr = self.hsvrlst.get_row(data['hsvrid'])
            if not hsvr:
                # hsvrlst entry not found
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                
            try:
                #self.db_components.begin_transaction()
                #self.db_events.begin_transaction()
                self.__attachLogicalVolume(data)
                #self.db_events.commit()
                #self.db_components.commit()
            except xmlrpclib.Fault:
                raise
            except Exception, inst:
                logger.error(traceback.format_exc())
                #self.db_events.rollback()
                #self.db_components.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def detachLogicalVolume(self, data):
        try:
            lvol = self.__get_lvol_by_id_or_name(data)
            data['lvolid'] = lvol['lvolid']

            try:
                #self.db_components.begin_transaction()
                self.__detachLogicalVolume(data)
                #self.db_components.commit()
            except xmlrpclib.Fault:
                raise
            except Exception, inst:
                logger.error(traceback.format_exc())
                #self.db_components.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

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
                if not self.ssvrlst.get_row(data['ssvrid']):
                    # ssvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                ssvrid = data['ssvrid']

            dskspace = self.lv.get_dskspace()
            dskspace_ssvr = dskspace['ssvr']

            ssvrs = self.ssvrlst.get_rows('ssvrid <> 0', '*')
            for ssvr in ssvrs:
                if ssvrid and ssvrid != ssvr['ssvrid']:
                    continue

                if dskspace_ssvr.has_key(ssvr['ssvrid']):
                    available, capacity = dskspace_ssvr[ssvr['ssvrid']]
                else:
                    available = 0
                    capacity = 0
                entry = {'ssvrid': ssvr['ssvrid'], 'priority': ssvr['priority'], \
                'ip_data': (ssvr['ip_data_1'], ssvr['ip_data_2']), 'resync': ssvr['resync'], \
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
                if not self.ssvrlst.get_row(data['ssvrid']):
                    # ssvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                ssvrid = data['ssvrid']

            dskspace = self.lv.get_dskspace()
            dskspace_dsk = dskspace['pdsk']

            pdsks = self.pdsklst.get_rows('pdskid <> 0', '*')
            for pdsk in pdsks:
                if ssvrid and ssvrid != pdsk['ssvrid']:
                    continue

                if dskspace_dsk.has_key(pdsk['pdskid']):
                    available = dskspace_dsk[pdsk['pdskid']]
                else:
                    available = 0
                entry = {'ssvrid': pdsk['ssvrid'], 'pdskid': pdsk['pdskid'], 'priority': pdsk['priority'], \
                'iscsi_path': (pdsk['iscsi_path_1'], pdsk['iscsi_path_2']), 'srp_name': pdsk['srp_name'], \
                'local_path': pdsk['local_path'], 'resync': pdsk['resync'], 'capacity': pdsk['capacity'],  \
                'available': available}
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
                if not self.hsvrlst.get_row(data['hsvrid']):
                    # hsvrlst entry not found
                    raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
                hsvrid = data['hsvrid']

            hsvrs = self.hsvrlst.get_rows('hsvrid <> 0', '*')
            for hsvr in hsvrs:
                if hsvrid and hsvrid != hsvr['hsvrid']:
                    continue

                entry = {'hsvrid': hsvr['hsvrid'], 'priority': hsvr['priority'], \
                'ip_data': (hsvr['ip_data_1'], hsvr['ip_data_2']), 'resync': hsvr['resync']}
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
        try:
            transits = self.transit.get_rows('eventid <> 0', '*')
            for transit in transits:
                entry = {'eventid': transit['eventid'], 'target': transit['target'], \
                'targetid': transit['id'], 'event': transit['event'], 'event_status': transit['status']}
                array.append(entry)

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return array

    def __scan_invalid_dexts(self, lvolid):
        lvolstruct = self.lv.get_lvolstruct(lvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, vas_db.LVOLTYPE['LINEAR'])

        for lovlstruct_mirror in lvolstruct_linear['components']:
            for lvolstruct_dext in lovlstruct_mirror['components']:
                if lvolstruct_dext['lvolspec']['status'] != vas_db.MIRROR_STATUS['VALID']:
                    return True
        return False

    def listLogicalVolumes(self, data):
        "list all logical volumes in the system."
        array = []
        try:
            lvolid = 0
            if data.has_key('lvolid') or data.has_key('lvolname'):
                lvol = self.__get_lvol_by_id_or_name(data)
                lvolid = lvol['lvolid']

            lvols = self.lvollst.get_rows('lvolid <> 0', '*')
            for lvol in lvols:

                if lvolid and lvolid != lvol['lvolid']:
                    continue

                lvolmap = self.lvolmap.get_row(lvol['lvolid'])

                fault = self.__scan_invalid_dexts(lvol['lvolid'])

                entry = {'hsvrid': lvol['hsvrid'], 'lvolid': lvol['lvolid'], \
                'lvolname': lvol['lvolname'], \
                'redundancy': lvol['redundancy'], \
                'capacity': lvol['capacity'], \
                'fault': fault, 'bind_status': lvolmap['status']}
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
            lvolstruct = self.lv.get_lvolstruct(lvol['lvolid'])

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return lvolstruct

    def __detachAllLogicalVolumesOfFailedHeadServer(self, hsvrid):
        # detach all lvols attached the failed head server.
        lvols = self.lvollst.get_rows('hsvrid', hsvrid)
        for lvol in lvols:
            self.lvollst.update_value('lvolid', lvol['lvolid'], 'hsvrid', 0)
            self.lv.unbound(lvol['lvolid'])

    def __registerHeadServerTransit(self, data):
        "register an event of head server state transition." 
        hsvrid = data['targetid']
        hsvr = self.hsvrlst.get_row(hsvrid)
        if not hsvr:
            return 0
        if hsvr['priority'] == vas_db.ALLOC_PRIORITY['OFFLINE']:
            return 0

        try:
            self.db_events.begin_transaction()
            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new eventid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            
            self.transit.put_row((eventid, vas_db.TARGET['HSVR'], hsvrid, vas_db.EVENT['ABNORMAL'], vas_db.EVENT_STATUS['OPEN']))
            self.db_events.commit()
        except Exception, inst:
            self.db_events.rollback()
            raise

        try:
            self.db_components.begin_transaction()

            # retrive pending resync requests on a failed head server
            self.db_events.begin_transaction()
            self.__retrieve_pending_resyncs(0, hsvrid)
            self.db_events.commit()

            # change status of the head server to EVACUATE.
            self.hsvrlst.update_value('hsvrid', hsvrid, 'priority', vas_db.ALLOC_PRIORITY['EVACUATE'])

            # detach all logical volumes without XML-RPC calls
            self.__detachAllLogicalVolumesOfFailedHeadServer(hsvrid)

            # change status of the head server to OFFLINE.
            self.hsvrlst.update_value('hsvrid', hsvrid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])
            self.db_components.commit()

            self.db_events.begin_transaction()
            self.transit.delete_rows('eventid', eventid)
            self.genid_event.recover(eventid)
            self.db_events.commit()

        except Exception, inst:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        return eventid

    def __evacuate_dext(self, dskmap_row, post_status):
        if dskmap_row['status'] == vas_db.EXT_STATUS['BUSY']:
            # replace 'in-use' disk extents. 'BUSY' --> 'EVACUATE'
            self.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['EVACUATE'])
            return True
        elif dskmap_row['status'] in (vas_db.EXT_STATUS['FREE'], vas_db.EXT_STATUS['OFFLINE']):
            if post_status == vas_db.EXT_STATUS['FAULTY']:
                # disable 'not-in-use' disk extents. 'FREE', 'OFFLINE' --> 'FAULTY'
                self.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])
        return False

    def __faulty_dext(self, dskmap_row):
        if dskmap_row['status'] == vas_db.EXT_STATUS['EVACUATE']:
            self.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['FAULTY'])

    def __offline_dext(self, dskmap_row):
        if dskmap_row['status'] == vas_db.EXT_STATUS['EVACUATE']:
            self.dskmap.update_value('dextid', dskmap_row['dextid'], 'status', vas_db.EXT_STATUS['OFFLINE'])

    def __evacuate_dsk(self,pdskid, post_status):
        # change status of all disk extents in the disk to EVACUATE too.
        need_recovering_mirror = False
        dskmap_rows = self.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if self.__evacuate_dext(dskmap_row, post_status):
                need_recovering_mirror = True
        return need_recovering_mirror

    def __faulty_dsk(self,pdskid):
        # change status of all disk extents in the disk to FAULTY.
        dskmap_rows = self.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            self.__faulty_dext(dskmap_row)

    def __offline_dsk(self,pdskid):
        # change status of all disk extents in the disk to OFFLINE.
        dskmap_rows = self.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            self.__offline_dext(dskmap_row)

    def __scan_ext_status(self,pdskid):
        # scan faulty dexts then decide priority of the disk.
        dskmap_rows = self.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if dskmap_row['status'] == vas_db.EXT_STATUS['FAULTY']:
                return vas_db.ALLOC_PRIORITY['LOW']
        return vas_db.ALLOC_PRIORITY['HIGH']

    def __shred_offline_exts(self,pdskid):
        # clearing offline disk extents with 0s to reuse
        dextids = []
        dskmap_rows = self.dskmap.get_rows('pdskid', pdskid)
        for dskmap_row in dskmap_rows:
            if dskmap_row['status'] == vas_db.EXT_STATUS['OFFLINE']:
                dextids.append(dskmap_row['dextid'])
        if len(dextids) != 0:
            self.__shredDiskExtent(dextids)

    def __replace_dext_array(self, dskmap_rows, post_status):
        # replace a set of disk extents
        need_recovering_mirror = False
        for dskmap_row in dskmap_rows:
            if self.__evacuate_dext(dskmap_row, post_status):
                need_recovering_mirror = True

        if need_recovering_mirror:
            self.__recovering_mirror(dskmap_rows[0]['pdskid'])
            for dskmap_row in dskmap_rows:
                dskmap_row = self.dskmap.get_row(dskmap_row['dextid'])
                self.__faulty_dext(dskmap_row)

    def __replace_dext(self,lvolid, post_status):
        # replace single disk extent
        dskmap_row = self.dskmap.get_row(lvolid)
        pdskid = dskmap_row['pdskid']
        if self.__evacuate_dext(dskmap_row, post_status):
            self.__recovering_mirror(pdskid)
            dskmap_row = self.dskmap.get_row(lvolid)
            self.__faulty_dext(dskmap_row)

    def __replace_dsk(self,pdskid):
        # replace all disk extent in a disk
        pdsklst_row = self.pdsklst.get_row(pdskid)
        assert(pdsklst_row['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY'])), lineno()

        # change the disk priority to EVACUATE.
        self.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['EVACUATE'])

        # change status of all disk extents in the disk to EVACUATE too.
        if self.__evacuate_dsk(pdskid, vas_db.EXT_STATUS['FAULTY']):
            # recovering downgraded mirror volumes.
            self.__recovering_mirror(pdskid)
            # change status of all disk extents in the disk to FAULTY.
            self.__faulty_dsk(pdskid)

        # change the disk priority to FAULTY too.
        self.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['FAULTY'])

    def __replace_ssvr(self, ssvrid):
        # replace all disk extent in a storage server
        ssvrlst_row = self.ssvrlst.get_row(ssvrid)
        assert(ssvrlst_row['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY'])), lineno()

        # change the storage server priority to EVACUATE.
        self.ssvrlst.update_value('ssvrid', ssvrid, 'priority', vas_db.ALLOC_PRIORITY['EVACUATE'])

        # change priority of all disks in the storage server to EVACUATE too.
        pdsklst_rows = self.pdsklst.get_rows('ssvrid', ssvrid)
        for pdsklst_row in pdsklst_rows:
            pdskid = pdsklst_row['pdskid']
            self.pdsklst.update_value('pdskid',  pdskid, 'priority', vas_db.ALLOC_PRIORITY['EVACUATE'])

            # change status of all disk extents in the disk to EVACUATE too.
            if self.__evacuate_dsk(pdskid, vas_db.EXT_STATUS['OFFLINE']):
                # recovering downgraded mirror volumes.
                self.__recovering_mirror(pdskid)

            # change priority of all disks in the storage server to OFFLINE.
            self.pdsklst.update_value('pdskid', pdskid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])

            # change status of all disk extents in the disk to OFFLINE.
            self.__offline_dsk(pdskid)

        # change the storage server priority to OFFLINE too.
        self.ssvrlst.update_value('ssvrid', ssvrid, 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])

    def __registerStorageServerTransit(self, data):
        "register an event of storage server state transition." 
        ssvrlst_row = self.ssvrlst.get_row(data['targetid'])
        if not ssvrlst_row:
            return 0
        # if priority of the storage server is already OFFLINE, nothing to do.
        if ssvrlst_row['priority'] == vas_db.ALLOC_PRIORITY['OFFLINE']:
            return 0

        try:
            self.db_events.begin_transaction()
            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new eventid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            self.transit.put_row((eventid, vas_db.TARGET['SSVR'], data['targetid'], vas_db.EVENT['ABNORMAL'], vas_db.EVENT_STATUS['OPEN']))
            self.db_events.commit()
        except Exception, inst:
            self.db_events.rollback()
            raise

        try:
            self.db_components.begin_transaction()

            self.db_events.begin_transaction()
            self.__retrieve_pending_shreds(0, data['targetid'])
            self.__replace_ssvr(data['targetid'])
            self.db_events.commit()

            self.db_events.begin_transaction()
            self.transit.delete_rows('eventid', eventid)
            self.genid_event.recover(eventid)
            self.db_events.commit()

            self.db_components.commit()
        except Exception, inst:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        return eventid

    def __registerDiskTransit(self, data):
        "register an event of disk state transition." 
        pdsklst_row = self.pdsklst.get_row(data['targetid'])
        if not pdsklst_row:
            return 0
        # if priority of the physical disk is already OFFLINE or FAULTY, nothing to do.
        if pdsklst_row['priority'] in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
            return 0

        try:
            self.db_events.begin_transaction()
            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new eventid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            self.transit.put_row((eventid, vas_db.TARGET['PDSK'], data['targetid'], vas_db.EVENT['ABNORMAL'], vas_db.EVENT_STATUS['OPEN']))
            self.db_events.commit()

            logger.debug("__registerDiskTransit: event: %d %s %d %s %s" % (eventid, 'PDSK', data['targetid'], 'ABNORMAL', 'OPEN'))
        except Exception, inst:
            self.db_events.rollback()
            raise
        try:
            self.db_components.begin_transaction()

            self.db_events.begin_transaction()
            self.__replace_dsk(data['targetid'])
            self.db_events.commit()

            self.db_events.begin_transaction()
            self.transit.delete_rows('eventid', eventid)
            self.genid_event.recover(eventid)
            self.db_events.commit()
            self.db_components.commit()

        except Exception, inst:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        return eventid

    def notifyFailure(self, data):
        # ret: eventid
        try:
            if not data.has_key('target'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if not data.has_key('targetid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            if data['target'] == vas_db.TARGET['HSVR']:
                return self.__registerHeadServerTransit(data)
            elif data['target'] == vas_db.TARGET['SSVR']:
                return self.__registerStorageServerTransit(data)
            elif data['target'] == vas_db.TARGET['PDSK']:
                return self.__registerDiskTransit(data)
            elif data['target'] == vas_db.TARGET['LVOL']:
                return self.__registerDextTransit(data)
            else:
                # invalid target
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __registerDextTransit(self, data):
        "register an event of disk state transition." 

        # lvolid(dext) --> pdskid
        dskmap_row = self.dskmap.get_row(data['targetid'])
        if not dskmap_row:
            raise Exception, "__registerDextTransit: disk extent(dext-%08x) is not found on the data base." % (data['targetid'])

        pdskid = dskmap_row['pdskid']

        # if status of the disk extent is already FAULTY, nothing to do.
        if dskmap_row['status'] == vas_db.EXT_STATUS['FAULTY']:
            logger.debug("the dext(dext-%08x) is already in FAULTY status. the event is ommited." % (data['targetid']))
            return 0

        # if priority of the disk is already FAULTY, nothing to do.
        pdsklst_row = self.pdsklst.get_row(pdskid)
        if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['FAULTY']:
            logger.debug("the disk(pdsk-%08x) is already in FAULTY status. the event is ommited." % (pdskid))
            return 0

        try:
            self.db_events.begin_transaction()
            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new eventid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            self.transit.put_row((eventid, vas_db.TARGET['LVOL'], data['targetid'], vas_db.EVENT['ABNORMAL'], vas_db.EVENT_STATUS['OPEN']))
            self.db_events.commit()
        except Exception, inst:
            self.db_events.rollback()
            raise

        try:
            self.db_components.begin_transaction()

            self.db_events.begin_transaction()
            self.__replace_dext(data['targetid'], vas_db.EXT_STATUS['FAULTY'])
            self.db_events.commit()

            if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['HIGH']:
                # HIGH --> HIGH/LOW
                self.pdsklst.update_value('pdskid', pdskid, 'priority', self.__scan_ext_status(pdskid))

            self.db_events.begin_transaction()
            self.transit.delete_rows('eventid', eventid)
            self.genid_event.recover(eventid)
            self.db_events.commit()

            self.db_components.commit()
        except Exception, inst:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        return eventid

    def notifyBadBlocks(self, data):
        "register bad blocks on a disk." 

        try:
            if not data.has_key('pdskid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if not data.has_key('blocks'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            pdskid = data['pdskid']

            # if priority of the disk is already FAULTY, nothing to do.
            pdsklst_row = self.pdsklst.get_row(pdskid)
            if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['FAULTY']:
                logger.debug("notifyBadBlocks: the disk(pdsk-%08x) is already in FAULTY status. the event is ommited." % (pdskid))
                return 0

            dskmap_rows=[]
            for block in data['blocks']:
                dskmap_row = self.dskmap.get_extent(pdskid, block)
                if not dskmap_row:
                    # disk extent is not found on the data base
                    raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                # if status of the disk extent is already FAULTY, nothing to do.
                if dskmap_row['status'] == vas_db.EXT_STATUS['FAULTY']:
                    logger.debug("notifyBadBlocks: the block(%d@pdsk-%08x) is already in FAULTY status. the block is skipped." % (block, pdskid))
                    continue
                dskmap_rows += [dskmap_row]    
            
            if len(dskmap_rows) == 0:
                return 0

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db_events.begin_transaction()
            eventid = self.genid_event.genid()
            if not eventid:
                # can not get new ssvrid
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
            self.transit.put_row((eventid, vas_db.TARGET['PDSK'] ,pdskid, vas_db.EVENT['ABNORMAL'], vas_db.EVENT_STATUS['OPEN']))
            self.db_events.commit()
        except xmlrpclib.Fault:
            self.db_events.rollback()
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_events.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db_components.begin_transaction()

            self.db_events.begin_transaction()
            self.__replace_dext_array(dskmap_rows, vas_db.EXT_STATUS['FAULTY'])
            self.db_events.commit()

            if pdsklst_row['priority'] == vas_db.ALLOC_PRIORITY['HIGH']:
                # HIGH --> HIGH/LOW
                self.pdsklst.update_value('pdskid', pdskid, 'priority', self.__scan_ext_status(pdskid))

            self.db_events.begin_transaction()
            self.transit.delete_rows('eventid', eventid)
            self.genid_event.recover(eventid)
            self.db_events.commit()

            self.db_components.commit()
        except xmlrpclib.Fault:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_events.rollback()
            self.db_components.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return eventid

    def notifyShredFinished(self, data):
        "data on a disk extent is shredded and overwritten with zero."
        try:
            if not data.has_key('dextid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            lvolid = data['dextid']
            dskmap = self.dskmap.get_row(lvolid)
            if not dskmap:
                # shredded dext is not found on the data base
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')
            if dskmap['status'] not in (vas_db.EXT_STATUS['OFFLINE'],vas_db.EXT_STATUS['FAULTY'], vas_db.EXT_STATUS['FREE']):
                # shredded dext is not offline
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db_components.begin_transaction()
            # clearing of an disk extent
            if dskmap['status'] == vas_db.EXT_STATUS['OFFLINE']:
                self.dskmap.update_value('dextid', lvolid, 'status', vas_db.EXT_STATUS['FREE'])
            self.db_components.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_components.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            # update shred dext event status. PROGRESS to CLOSED
            events = self.transit.get_rows('status',vas_db.EVENT_STATUS['PROGRESS'])
            for event in events:
                if event['event'] != vas_db.EVENT['SHRED']:
                    continue
                # id of SHRED event is lvolid of disk extent to be zero cleared
                if event['id'] == lvolid:
                    self.db_events.begin_transaction()
                    # delete finished shred task
                    self.shred.delete_rows('eventid', event['eventid'])

                    # update shred dext event status. PROGRESS to CLOSED
                    self.transit.delete_rows('eventid', event['eventid'])
                    self.genid_event.recover(event['eventid'])
                    self.db_events.commit()

            # send a shred request to ssvr_agent
            self.db_events.begin_transaction()
            self.__shredDiskExtent_post(MAX_SHRED_TASKS)
            self.db_events.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_events.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __check_ip_data(self, data):
        ip_addr_re = re.compile("^\d+\.\d+\.\d+\.\d+$")
        if not data.has_key('ip_data'):
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        if len(data['ip_data']) != 2:
            raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        for ip_data in data['ip_data']:
            if not ip_addr_re.match(ip_data):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

    def registerHeadServer(self, data):
        "register a head server." 
        try:
            self.__check_ip_data(data)
            hsvr = self.hsvrlst.get_svr_by_ip_data(data['ip_data'])
            if hsvr:
                # the head server is already registered
                if hsvr['priority'] != vas_db.ALLOC_PRIORITY['OFFLINE']:
                    # EEXIST error makes head server shutdown
                    raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
                hsvrid = hsvr['hsvrid']
            else:
                hsvrid = self.genid_hsvr.genid()
                if not hsvrid:
                    # can not get new ssvrid
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            if str(inst) == "IP address confliction":
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            if hsvr:
                self.db_components.begin_transaction()
                # OFFLINE --> HIGH
                self.hsvrlst.update_value('hsvrid', hsvrid, 'priority', vas_db.ALLOC_PRIORITY['HIGH'])
            else:
                # inTransaction
                # NONE --> HIGH
                self.hsvrlst.put_row((hsvrid, vas_db.ALLOC_PRIORITY['HIGH'], data['ip_data'][0], 0, 0, data['ip_data'][1]))
            self.db_components.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_components.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return hsvrid

    def registerStorageServer(self, data):
        "register a storage server." 
        try:
            self.__check_ip_data(data)
            ssvr = self.ssvrlst.get_svr_by_ip_data(data['ip_data'])
            if ssvr:
                # the storage server is already registered
                if ssvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['HALT']):
                    # EEXIST error makes storage server shutdown
                    raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
                ssvrid = ssvr['ssvrid']
            else:
                ssvrid = self.genid_ssvr.genid()
                if not ssvrid:
                    # can not get new ssvrid
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            if str(inst) == "IP address confliction":
                raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            if ssvr:
                self.db_components.begin_transaction()
                # OFFLINE --> HIGH
                self.ssvrlst.update_value('ssvrid', ssvrid, 'priority', vas_db.ALLOC_PRIORITY['HIGH'])
            else:
                # inTransaction
                # NONE --> HIGH
                self.ssvrlst.put_row((ssvrid, vas_db.ALLOC_PRIORITY['HIGH'], data['ip_data'][0], 0, 0, data['ip_data'][1]))
            self.db_components.commit()
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_components.rollback()
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

            ssvr = self.ssvrlst.get_row(data['ssvrid'])
            if not ssvr:
                # invalid ssvrid
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if ssvr['priority'] != vas_db.ALLOC_PRIORITY['HIGH']:
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            pdsk_is_offline = False
            pdskid = 0
            dsks = self.pdsklst.get_rows('ssvrid',data['ssvrid'])
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
                pdskid = self.genid_dsk.genid()
                if not pdskid:
                    # can not get new pdskid
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
                lvolid = self.genid_lvol.genid()
                if not lvolid:
                    # can not get new lvolid
                    self.db_components.rollback()
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
                superid = self.genid_lvol.genid()
                if not superid:
                    # can not get new lvolid
                    self.db_components.rollback()
                    raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            if pdsk_is_offline:
                self.db_components.begin_transaction()
                # OFFLINE --> HIGH/LOW
                self.pdsklst.update_value('pdskid', pdskid, 'priority', self.__scan_ext_status(pdskid))

                # clearing offline disk extents with 0s to reuse
                self.db_events.begin_transaction()
                self.__shred_offline_exts(pdskid)
                self.db_events.commit()
            else:
                # inTransaction
                # NONE --> HIGH

                # data['capacity'] is size in sector_str
                size_in_sector = int(data['capacity'], 10)
                if size_in_sector <= 0:
                    raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
                size_in_giga, odd = stovb(size_in_sector)

                # brand-new disk.
                # clearing of an entire physical disk and divide the sectors onto a free disk extent for data and a super block for meta data.

                super_sectors = odd
                if super_sectors == 0:
                    size_in_giga -= 1
                    super_sectors = vbtos(1)
                if SCSI_DRIVER == 'iet':
                    iscsi_path_1 = ISCSI_PATH % (ssvr['ip_data_1'], iqn_prefix_iscsi,  pdskid)
                    iscsi_path_2 = ISCSI_PATH % (ssvr['ip_data_2'], iqn_prefix_iscsi,  pdskid)
                elif SCSI_DRIVER == 'srp':
                    iscsi_path_1 = iscsi_path_2 = SRP_PATH % ( data['srp_name'] )

                self.pdsklst.put_row((data['ssvrid'], pdskid, size_in_giga, iscsi_path_1, \
                   iscsi_path_2, data['srp_name'], data['local_path'], vas_db.ALLOC_PRIORITY['HIGH'], 0))
                self.dskmap.put_row((pdskid, lvolid, 0, size_in_giga, vas_db.EXT_STATUS['OFFLINE']))
                self.dskmap.put_row((pdskid, superid, size_in_giga, super_sectors, vas_db.EXT_STATUS['SUPER']))

                self.db_events.begin_transaction()
                self.__shredDiskExtent([lvolid])
                self.db_events.commit()

            self.db_components.commit()
        except xmlrpclib.Fault:
            # __shredDiskExtent error case
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        except Exception, inst:
            self.db_events.rollback()
            self.db_components.rollback()
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return pdskid

    def __dextid_to_ssvrid(self, lvolid):
        dskmap_row = self.dskmap.get_row(lvolid)
        pdsklst_row = self.pdsklst.get_row(dskmap_row['pdskid'])
        return pdsklst_row['ssvrid']

    def __dextid_to_pdskid(self, lvolid):
        dskmap_row = self.dskmap.get_row(lvolid)
        return dskmap_row['pdskid']

    def __dextid_to_hsvrid(self, lvolid):
        dext = self.lvolmap.get_row(lvolid)
        mirror = self.lvolmap.get_row(dext['superlvolid'])
        lvollst_row = self.lvollst.get_row(mirror['superlvolid'])
        return lvollst_row['hsvrid']

    def __mirrorid_to_hsvrid(self, lvolid):
        lvolmap_row = self.lvolmap.get_row(lvolid)
        lvollst_row = self.lvollst.get_row(lvolmap_row['superlvolid'])
        return lvollst_row['hsvrid']

    def notifyRebuildMirrorFinished(self, data):
        try:
            if not data.has_key('mirrorid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
            if not data.has_key('dexts'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            if not self.lvolmap.get_row(data['mirrorid']):
                # invalid mirrorid
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db_components.begin_transaction()
            self.lv.rebuildfinished_mirror(data['dexts'])

            # update resync mirror event status. PROGRESS to CLOSED
            events = self.transit.get_rows('status',vas_db.EVENT_STATUS['PROGRESS'])
            for event in events:
                if event['event'] != vas_db.EVENT['RESYNC']:
                    continue
                # id of RESYNC event is lvolid of spare disk extent to be resynced
                for dext in data['dexts']:
                    if dext['dext_status'] == vas_db.MIRROR_STATUS['VALID'] and event['id'] == dext['dextid']:

                        # finish resync task
                        self.db_events.begin_transaction()
                        self.__close_resync(self.resync.get_row(event['eventid']))
                        self.db_events.commit()

            # send a resync request to hsvr_agent
            self.db_events.begin_transaction()
            self.__recovering_mirror_post(MAX_RESYNC_TASKS)
            self.db_events.commit()

            # send a shred request to ssvr_agent
            self.db_events.begin_transaction()
            self.__shredDiskExtent_post(MAX_SHRED_TASKS)
            self.db_events.commit()
            self.db_components.commit()    
        except xmlrpclib.Fault:
            self.db_events.rollback()
            self.db_components.rollback()
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_events.rollback()
            self.db_components.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def deletePhysicalDisk(self, data):
        try:
            if not data.has_key('pdskid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            pdskid = data['pdskid']

            if not self.pdsklst.get_row(pdskid):
                # pdsk entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # check physical disk is offline
            try:
                self.__assert_dsk_offline(pdskid)
            except Exception, inst:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            
            # cancel pending shred requests on the disk
            try:
                self.db_events.begin_transaction()
                self.__retrieve_pending_shreds(pdskid, 0)
                self.db_events.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_events.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

            # delete a entry from db
            try:
                self.db_components.begin_transaction()
                self.__delete_offline_disk(pdskid)
                self.db_components.commit()    
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_components.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def deleteHeadServer(self, data):
        try:
            if not data.has_key('hsvrid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            hsvrid = data['hsvrid']
            hsvr = self.hsvrlst.get_row(hsvrid)
            if not hsvr:
                # hsvrlst entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # chech the head server is offline
            if hsvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            # check no logical volumes attached to the head server
            assert len(self.lvollst.get_rows('hsvrid',hsvrid)) == 0, lineno()

            # delete entries from DB
            try:
                self.db_components.begin_transaction()

                self.hsvrlst.delete_rows('hsvrid', hsvrid)

                self.db_components.commit()            
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_components.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def deleteStorageServer(self, data):
        try:
            if not data.has_key('ssvrid'):
                raise xmlrpclib.Fault(errno.EINVAL, 'EINVAL')

            ssvrid = data['ssvrid']
            ssvr = self.ssvrlst.get_row(ssvrid)
            if not ssvr:
                # ssvrlst entry not found
                raise xmlrpclib.Fault(errno.ENOENT, 'ENOENT')

            # chech the storage server is offline
            if ssvr['priority'] not in (vas_db.ALLOC_PRIORITY['OFFLINE'], vas_db.ALLOC_PRIORITY['FAULTY']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')

            # cancel pending shred requests on the disk
            try:
                self.db_events.begin_transaction()
                self.__retrieve_pending_shreds(0, ssvrid)
                self.db_events.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_events.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

            # delete entries from DB
            try:
                self.db_components.begin_transaction()
                dsks = self.pdsklst.get_rows('ssvrid', ssvrid)
                for dsk in dsks:
                    self.__delete_offline_disk(dsk['pdskid'])

                self.ssvrlst.delete_rows('ssvrid', ssvrid)
                self.db_components.commit()
            except Exception, inst:
                logger.error(traceback.format_exc())
                self.db_components.rollback()
                raise xmlrpclib.Fault(500, 'Internal Server Error')

        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __offlineAll(self):
        for hsvr in self.hsvrlst.get_rows('hsvrid <> 0', '*'):
            if hsvr['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.hsvrlst.update_value('hsvrid', hsvr['hsvrid'], 'priority', vas_db.ALLOC_PRIORITY['OFFLINE'])
        for ssvr in self.ssvrlst.get_rows('ssvrid <> 0', '*'):
            if ssvr['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.ssvrlst.update_value('ssvrid', ssvr['ssvrid'], 'priority', vas_db.ALLOC_PRIORITY['HALT'])
        for pdsk in self.pdsklst.get_rows('pdskid <> 0', '*'):
            if pdsk['priority'] in (vas_db.ALLOC_PRIORITY['HIGH'], vas_db.ALLOC_PRIORITY['LOW']):
                self.pdsklst.update_value('pdskid', pdsk['pdskid'], 'priority', vas_db.ALLOC_PRIORITY['HALT'])

    def shutdownAll(self, data):
        try:
            # check existence of attached logical volumes
            if len(self.lvollst.get_rows('hsvrid <> 0', '*')) != 0:
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
            # check existence of shred tasks
            if self.transit.get_shreds(vas_db.EVENT_STATUS['PROGRESS']) or \
                self.transit.get_shreds(vas_db.EVENT_STATUS['PENDING']):
                raise xmlrpclib.Fault(errno.EBUSY, 'EBUSY')
        except xmlrpclib.Fault:
            raise
        except Exception, inst:
            logger.error(traceback.format_exc())
            raise xmlrpclib.Fault(500, 'Internal Server Error')

        try:
            self.db_components.begin_transaction()
            self.__offlineAll()
            self.db_components.commit()

        except Exception, inst:
            logger.error(traceback.format_exc())
            self.db_components.rollback()
            raise xmlrpclib.Fault(500, 'Internal Server Error')
        return 0

    def __call_request(self, ip_addrs, port, method, data):
        global request_table, request_event, request_lock
        x = {'ip_addrs':ip_addrs, 'port':port, 'method':method, 'data':data}
        request_lock.acquire()
        request_table.append(x)
        request_event.set()
        request_lock.release()

    def __send_request(self, ip_addrs, port, method, data):
        for ip in ip_addrs:
            try:
                agent = xmlrpclib.ServerProxy("http://%s:%s" % (ip, port))
                func = getattr(agent, method)
                res = func(data)
                return res
            except socket.timeout, inst:
                # timeout
                logger.error("__send_request: timeout: %s", (inst))
                raise xmlrpclib.Fault(errno.ETIMEDOUT, 'ETIMEDOUT')
            except xmlrpclib.Fault, inst:
                # Exceptions on remote side
                raise
            except Exception, inst:
                # try other link
                continue
        # both link down or server down
        raise xmlrpclib.Fault(500, 'Internal Server Error')

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

class RequestWorker(threading.Thread):
    def __init__(self, n):
        threading.Thread.__init__(self)
        self.id = n;

    def run(self):
        global request_table, request_event, request_lock
        while True:
            request_event.wait()
            request_lock.acquire()
            x = None
            if len(request_table) > 0:
                x = request_table.pop()
            else:
                request_event.clear()
            request_lock.release()
            if x is not None:
		try:
		    self.__send_request(x['ip_addrs'], x['port'], x['method'], \
			x['data'])
                except:
		    # XXX should implement more appropriate error handling.
		    # sleeping here can block unrelated tasks.
		    time.sleep(1)
		    request_lock.acquire()
		    request_table.append(x)
		    request_event.set()
		    request_lock.release()

    # set timeout large value !! (60s or such)
    def __send_request(self, ip_addrs, port, method, data):
        logger.debug("RW.__send_request %s %s %s %s" % \
	    (ip_addrs, port, method, data))
        for ip in ip_addrs:
            try:
                agent = xmlrpclib.ServerProxy("http://%s:%s" % (ip, port))
                func = getattr(agent, method)
                res = func(data)
                logger.info("RW.__send_request: %s respons: %s" % (ip, res))
                return
            except socket.timeout, inst:
                # timeout
                logger.info("RW.__send_request: %s timeout: %s", (ip, inst))
                # but OK, Server doing job
                return
            except xmlrpclib.Fault, inst:
                # Exceptions on remote side
                logger.error("RW.__send_request: %s Exceptions on remote side: %s", (ip, inst))
                return
            except Exception, inst:
                # try other link
		logger.debug("RW.__send_request %s failed %s" % (ip, inst))
                continue
        logger.error("RW.__send_request: both link down or server down")
	raise Exception, "both link down"

class Tserver(ThreadingMixIn, SimpleXMLRPCServer.SimpleXMLRPCServer):
    pass

def main():
    global main_lock, request_table, request_event, request_lock, storage_manager_object
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

    storage_manager_object = StorageManager()

    try:
        storage_manager_object.connect_db()
    except:
        logger.error(traceback.format_exc())
        sys.exit(1)

    socket.setdefaulttimeout(SOCKET_DEFAULT_TIMEOUT)

    request_workers = []
    request_table = []
    request_event = threading.Event()
    request_lock = threading.Lock()
    for i in range(STORAGE_MANAGER_REQUEST_WORKERS):
        worker = RequestWorker(i)
        request_workers.append(worker)
        worker.setDaemon(True)
        worker.start()

    main_lock = threading.Lock()

    Tserver.allow_reuse_address=True
    server = Tserver((host, port_storage_manager))
    server.register_instance(storage_manager_object)

    server.register_introspection_functions()

    #Go into the main listener loop
    print "Listening on port %s" % port_storage_manager
    server.serve_forever()

if __name__ == "__main__":
    main()
