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

__version__ = '$Id: lvnode.py 161 2010-08-16 06:59:29Z yamamoto2 $'

import datetime
import dag
from vas_conf import *
from event import EVENT_STATUS
from vas_const import LVOLTYPE, EXT_STATUS, TARGET, \
    MIRROR_STATUS, ALLOC_PRIORITY
from vas_subr import get_iscsi_path, lineno

# XXX little point to inherit DAGNode
class LVDbNode(dag.DAGNode):
    def __init__(self, upper):
        dag.DAGNode.__init__(self)
        if upper:
            self.add_antecedent(upper)

    def get_lvolspec(self, db, lvolid):
        return {}

    def create(self, db, lvinfo, lvmap):
        pass

    def delete(self, db, lvolid):
        pass

    def attach(self, db, lvolid):
        pass

    def detach(self, db, lvolid):
        pass

class SnapshotOriginDbNode(LVDbNode):
    def create(self, db, lvinfo, lvmap):
        # assert in transaction
        lvolid = db.genid_lvol()
        capacity = lvmap['capacity']
        db.lvolmap.put_row(((lvolid, LVOLTYPE['SNAPSHOT-ORIGIN'], \
            lvmap['upperid'], 0, capacity, lvinfo['lvolid'], 0)))
        dbnode = self.succedents[0]
        map = {'upperid': lvolid, 'offset': 0, 'capacity': capacity}
        dbnode.create(db, lvinfo, map)
        return lvolid

class SnapshotDbNode(LVDbNode):
    def create(self, db, lvinfo, lvmap):
        # assert in transaction
        lvolid = db.genid_lvol()
        capacity = lvmap['capacity']
        db.lvolmap.put_row(((lvolid, LVOLTYPE['SNAPSHOT'], \
            lvmap['upperid'], 0, capacity, lvinfo['lvolid'], 0)))
        dbnode = self.succedents[0]
        map = {'upperid': lvolid, 'offset': 0, 'capacity': capacity}
        dbnode.create(db, lvinfo, map)
        return lvolid
    def get_lvolspec(self, db, lvolid):
        snapshot_row = db.snapshot.get_rows('snapshot_lvolid', lvolid)
        if len(snapshot_row) == 0:
            # we are in the middle of createSnapshot
            return {}
        assert(len(snapshot_row) == 1)
        return { \
            'origin_lvolid' : snapshot_row[0]['origin_lvolid'] \
        }
    def delete(self, db, lvolid):
        snapshot_row = db.snapshot.get_rows('snapshot_lvolid', lvolid)
        assert(len(snapshot_row) == 1)
        db.snapshot.delete_rows('snapshot_lvolid', lvolid)

class LinearDbNode(LVDbNode):
    def create(self, db, lvinfo, lvmap):
        # assert in transaction
        lvolid = db.genid_lvol()
        capacity = lvmap['capacity']
        db.lvolmap.put_row(((lvolid, LVOLTYPE['LINEAR'], lvmap['upperid'], \
            0, capacity, lvinfo['lvolid'], 0)))
        resid = capacity
        offset = 0
        dbnode = self.succedents[0]
        for extlen in EXTENTSIZE:
            while resid / extlen > 0:
                map = {'upperid': lvolid, 'offset': offset, 'capacity': extlen}
                dbnode.create(db, lvinfo, map)
                resid -= extlen
                offset += extlen
        assert(resid == 0)
        return lvolid

class MirrorDbNode(LVDbNode):
    def create(self, db, lvinfo, lvmap):
        # assert in transaction
        mirrorid = db.genid_lvol()
        db.lvolmap.put_row(((mirrorid, LVOLTYPE['MIRROR'], lvmap['upperid'], \
            lvmap['offset'], lvmap['capacity'], lvinfo['lvolid'], 0)))
        # XXX: will be fixed (after merge snapshot)
        if self.succedents:
            dbnode = self.succedents[0]
        else:
            dbnode = get_dbnode(LVOLTYPE['DEXT']) # expand case
        omit = []
        for i in range(lvinfo['redundancy']):
            map = {'upperid': mirrorid, 'offset': 0, 'capacity': lvmap['capacity'], 'omit': omit}
            dextid = dbnode.create(db, lvinfo, map)
            omit.append(dextid)
        return mirrorid

class DextDbNode(LVDbNode):
    def get_lvolspec(self, db, lvolid):
        dskmap = db.dskmap.get_row(lvolid)
        pdsklst = db.pdsklst.get_row(dskmap['pdskid'])
        ip_addrs = db.ipdata.ssvr_ipdata(pdsklst['ssvrid'])
        ip_paths = []
        for ip in ip_addrs:
            path = get_iscsi_path(ip, dskmap['pdskid'], pdsklst['srp_name'])
            ip_paths.append(path)
        return {'pdskid': dskmap['pdskid'], 'offset': dskmap['offset'], \
            'ssvrid': pdsklst['ssvrid'], 'iscsi_path': ip_paths}

    def create(self, db, lvinfo, lvmap):
        # assert in transaction
        # assert no succedents
        dextid = db.allocate_dext(lvmap['capacity'], lvmap['omit'])
        db.lvolmap.put_row((dextid, LVOLTYPE['DEXT'], lvmap['upperid'], 0, \
            lvmap['capacity'], lvinfo['lvolid'], MIRROR_STATUS['ALLOCATED']))
        return dextid

    def delete(self, db, lvolid):
        lvolmap = db.lvolmap.get_row(lvolid)
        mirror_status = lvolmap['mirror_status']
        if mirror_status == MIRROR_STATUS['ALLOCATED']:
            db.dskmap.update_value('dextid', lvolid, 'status', EXT_STATUS['FREE'])
        else:
            db.dskmap.update_value('dextid', lvolid, 'status', EXT_STATUS['OFFLINE'])
            dskmap = db.dskmap.get_row(lvolid)
            dsk = db.pdsklst.get_row(dskmap['pdskid'])
            eventid = db.genid_event()
            db.shred.put_row((eventid, lvolid, dsk['pdskid'], dskmap['offset'], \
                dskmap['capacity'], EVENT_STATUS['PENDING']))
            logger.debug("DextDbNode delete: event: %d dextid %d" % (eventid, lvolid))

    def attach(self, db, lvolid):
        lvolmap = db.lvolmap.get_row(lvolid)
        mirror_status = lvolmap['mirror_status']
        if mirror_status in (MIRROR_STATUS['ALLOCATED'], MIRROR_STATUS['NEEDSHRED']):
            db.lvolmap.update_value('lvolid', lvolid, 'mirror_status', MIRROR_STATUS['INSYNC'])
        elif mirror_status == MIRROR_STATUS['SPARE']:
            # schedule resync
            lvolmap = db.lvolmap.get_row(lvolid)
            mirrorid = lvolmap['superlvolid']
            eventid = db.genid_event()
            db.resync.put_row((eventid, mirrorid, lvolid, 0, EVENT_STATUS['PENDING']))

    def detach(self, db, lvolid):
        lvolmap = db.lvolmap.get_row(lvolid)
        mirror_status = lvolmap['mirror_status']
        if mirror_status == MIRROR_STATUS['ALLOCATED']:
            # extent may be dirty. so mark NEEDSHRED.
            db.lvolmap.update_value('lvolid', lvolid, 'mirror_status', MIRROR_STATUS['NEEDSHRED'])
        elif mirror_status == MIRROR_STATUS['SPARE']:
            # delete resync row 
            db.resync.delete_rows('lvolid_add', lvolid)

def get_dbnode(lvoltype):
    if lvoltype == LVOLTYPE['SNAPSHOT-ORIGIN']:
        return SnapshotOriginDbNode(None)
    elif lvoltype == LVOLTYPE['SNAPSHOT']:
        return SnapshotDbNode(None)
    elif lvoltype == LVOLTYPE['LINEAR']:
        return LinearDbNode(None)
    elif lvoltype == LVOLTYPE['MIRROR']:
        return MirrorDbNode(None)
    elif lvoltype == LVOLTYPE['DEXT']:
        return DextDbNode(None)
    else:
        # raise
        pass

def get_lvolstruct(db, lvolid):
    def __get_lvolstruct(db, lvolmap, upper):
        node = get_dbnode(lvolmap['lvoltype'])
        spec = node.get_lvolspec(db, lvolmap['lvolid'])
        mirror_status = lvolmap['mirror_status']
        if mirror_status == MIRROR_STATUS['NEEDSHRED']:
            # NEEDSHRED is treated as ALLOCATED except delete
            mirror_status = MIRROR_STATUS['ALLOCATED']
        lvolstruct = { \
            'lvolid': lvolmap['lvolid'], \
            'lvoltype': lvolmap['lvoltype'], \
            'capacity': lvolmap['capacity'], \
            'mirror_status': mirror_status, \
            'lvolspec': spec, \
            'labels': [], \
            'components': [] \
        }
        child = db.lvolmap.get_rows('superlvolid', lvolmap['lvolid'])
        for c in child:
            __get_lvolstruct(db, c, lvolstruct)
        upper['components'].append(lvolstruct)
        return lvolstruct

    lvollst = db.lvollst.get_row(lvolid)
    lvolname = lvollst['lvolname']
    assoc = db.assoc.get_rows('lvolid', lvolid)
    assoc_lvolstructs = \
        map(lambda a: get_lvolstruct(db, a['assoc_lvolid']), assoc)
    spec = { \
        'lvolname': lvolname, \
        'redundancy': lvollst['redundancy'], \
        'ctime': lvollst['ctime'], \
        'assoc': assoc_lvolstructs \
    }
    labels = [ \
        lvolname, \
        "lvol-%08x" % lvolid \
    ]
    lvolstruct = { \
        'lvolid': lvolid, \
        'lvoltype': LVOLTYPE['LVOL'], \
        'capacity': lvollst['capacity'], \
        'mirror_status': 0, \
        'lvolspec': spec, \
        'labels': labels, \
        'components': [] \
    }
    top = db.lvolmap.get_rows('superlvolid', lvolid)
    assert(len(top) == 1)
    __get_lvolstruct(db, top[0], lvolstruct)
    return lvolstruct

# create is something special than other methods
# create method creates lvolmap and lvoltype specific records following dbnode chain
# node's create method call succedents create explicitly
# other methods do something lvoltype specific task follow lvolmap
def lvol_db_create(db, top, lvinfo):
    lvolid = db.genid_lvol()
    # creation time: ISO 8601 format string: YYYY-MM-DDThh:mm:ss (without microsec)
    ctime = datetime.datetime.utcnow().isoformat().split('.')[0]
    db.lvollst.put_row((lvolid, lvinfo['lvolname'], lvinfo['redundancy'], \
        lvinfo['capacity'], ctime, 0))
    lvinfo['lvolid'] = lvolid
    lvmap = {'upperid': lvolid, 'offset': 0, 'capacity': lvinfo['capacity']}
    top.create(db, lvinfo, lvmap)
    return lvolid

def lvol_db_delete(db, lvolid):
    def __lvol_db_delete(lvolmap):
        child = db.lvolmap.get_rows('superlvolid', lvolmap['lvolid'])
        for c in child:
            __lvol_db_delete(c)
        node = get_dbnode(lvolmap['lvoltype'])
        node.delete(db, lvolmap['lvolid'])
        # node.delete may refer lvolmap, so delete lvolmap last
        db.lvolmap.delete_rows('lvolid', lvolmap['lvolid'])

    # assert in transaction
    top = db.lvolmap.get_rows('superlvolid', lvolid)
    assert(len(top) == 1)
    __lvol_db_delete(top[0])
    db.lvollst.delete_rows('lvolid', lvolid)
    db.assoc.delete_rows('assoc_lvolid', lvolid)

def lvol_db_attach(db, lvolid):
    def __lvol_db_attach(lvolmap):
        child = db.lvolmap.get_rows('superlvolid', lvolmap['lvolid'])
        for c in child:
            __lvol_db_attach(c)
        node = get_dbnode(lvolmap['lvoltype'])
        node.attach(db, lvolmap['lvolid'])

    # assert in transaction
    top = db.lvolmap.get_rows('superlvolid', lvolid)
    assert(len(top) == 1)
    __lvol_db_attach(top[0])
    assoc = db.assoc.get_rows('lvolid', lvolid)
    for a in assoc:
        lvol_db_attach(db, a['assoc_lvolid'])

def lvol_db_detach(db, lvolid):
    def __lvol_db_detach(lvolmap):
        child = db.lvolmap.get_rows('superlvolid', lvolmap['lvolid'])
        for c in child:
            __lvol_db_detach(c)
        node = get_dbnode(lvolmap['lvoltype'])
        node.detach(db, lvolmap['lvolid'])

    # assert in transaction
    top = db.lvolmap.get_rows('superlvolid', lvolid)
    assert(len(top) == 1)
    __lvol_db_detach(top[0])
    assoc = db.assoc.get_rows('lvolid', lvolid)
    for a in assoc:
        lvol_db_detach(db, a['assoc_lvolid'])

    lvollst = db.lvollst.get_row(lvolid)
    assert(lvollst)
    if lvollst['deleted']:
        lvol_db_delete(db, lvolid)
