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

__version__ = '$Id: vas_db.py 321 2010-10-20 05:59:06Z yamamoto2 $'

import sys
import traceback
import types
from vas_conf import DB_COMPONENTS, EXTENTSIZE, logger, MAX_RESOURCEID, MAX_EVENTID, MAX_LENGTH, MAX_REDUNDANCY, MIN_REDUNDANCY
from vas_subr import lineno
from event import EVENT_STATUS
from vas_const import \
    ALLOC_PRIORITY, \
    ALLOC_PRIORITY_STR, \
    ATTACH_EVENT, \
    ATTACH_STATUS, \
    EXT_STATUS, \
    IDTYPE, \
    LVOLTYPE, \
    MIRROR_STATUS, \
    MIRROR_STATUS_STR, \
    TARGET, \
    TARGET_PREFIX

class Db_base:
    def __init__(self, path):
        self.conn = None
        self.c = None
        self.path = path
        self.tables = []

    def connect(self):
        if self.conn:
            return
        m = __import__("sqlite", globals(), locals(), [])
        self.conn = m.connect(self.path)
        self.c = self.conn.cursor()
        logger.info("loaded %s DB library, API version %s" % ("sqlite", m.apilevel))

    def disconnect(self):
        if not self.conn:
            return
        self.c.close()
        self.conn.close()
        self.conn = None

    def initialize_tables(self):
        for t in self.tables:
            t.initialize_table()

    def add_table(self, t):
        self.tables.append(t)

    def begin_transaction(self):
        assert(self.conn.inTransaction == 0), lineno()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

SSVRLST_DEF = {'name': 'ssvrlst', 'fields': [('ssvrid', 'int'), ('priority', 'int'), \
    ('latency', 'int')], \
    'primary_key': 'ssvrid'}
HSVRLST_DEF = {'name': 'hsvrlst', 'fields': [('hsvrid', 'int'), ('priority', 'int'), \
    ('latency', 'int')], \
    'primary_key': 'hsvrid'}
PDSKLST_DEF = {'name': 'pdsklst', 'fields': [('ssvrid', 'int'), ('pdskid', 'int'), \
    ('capacity', 'int'), ('local_path', 'varchar(1024)'), ('priority', 'int')], \
    'primary_key': 'pdskid'}
LVOLLST_DEF = {'name': 'lvollst', 'fields': [('lvolid', 'int'), \
    ('lvolname', 'varchar(64)'), ('redundancy', 'int'), ('capacity', 'int'), \
    ('ctime', 'varchar(20)'), ('deleted', 'int')], 'primary_key': 'lvolid'}
DSKMAP_DEF = {'name': 'dskmap', 'fields': [('pdskid', 'int'), ('dextid', 'int'), \
    ('offset', 'int'), ('capacity', 'int'), ('status', 'int')], \
    'primary_key': 'dextid'}
LVOLMAP_DEF = {'name': 'lvolmap', 'fields': [('lvolid', 'int'), ('lvoltype', 'int'), \
    ('superlvolid', 'int'), ('offset', 'int'), ('capacity', 'int'), \
    ('toplvolid', 'int'), ('mirror_status', 'int')], \
    'primary_key': 'lvolid'}
#
# snapshot: maintain associations between SNAPSHOT and SNAPSHOT-ORIGIN logical
# volumes
#
#       origin_lvolid: lvolid of SNAPSHOT-ORIGIN lvol
#       snapshot_lvolid: lvolid of SNAPSHOT lvol
#
SNAPSHOT_DEF = { \
    'name': 'snapshot', \
    'fields': [ \
        ('origin_lvolid', 'int'), \
        ('snapshot_lvolid', 'int'), \
    ], \
    'primary_key': 'snapshot_lvolid' \
    }
#
# assoc: maintain associations between 'LVOL' logical volumes
#
#       lvolid: lvolid of "associated_to" logical volume
#       assoc_lvolid: lvolid of "associated_from" logical volume
#       type: type of the association.
#
# whenever the "associated_to" volume is attached/detached,
# "associated_from" volumes are attached to/detached from the same
# head server.
# the "associated_to" volume can not be deleted unless all its
# "associated_from" volumes are deleted.
#
# in the case of snapshot,
#       lvolid: the logical volume on which the snapshot is taken
#       assoc_lvolid: the cow volume
#       type: "snapshot"
#
ASSOC_DEF = { \
    'name': 'assoc', \
    'fields': [ \
        ('lvolid', 'int'), \
        ('assoc_lvolid', 'int'), \
        ('type', 'varchar(64)'), \
    ], \
    'primary_key': 'assoc_lvolid' \
    }
RESYNC_DEF = {'name': 'resync', 'fields': [('eventid', 'int'), ('mirrorid', 'int'), \
    ('lvolid_add', 'int'), ('lvolid_rm', 'int'), ('status', 'int')], \
    'primary_key': 'eventid'}
SHRED_DEF = {'name': 'shred', 'fields': [('eventid', 'int'), ('dextid', 'int'), ('pdskid', 'int'), \
    ('offset', 'int'), ('capacity', 'int'), ('status', 'int')], \
    'primary_key': 'eventid'}
IPDATA_DEF = {'name': 'ipdata', 'fields': [('ip', 'varchar(20)'), ('hsvrid', 'int'), ('ssvrid', 'int')], \
    'primary_key': 'ip'} 
ATTACH_DEF = {'name': 'attach', 'fields': [('lvolid', 'int'), \
    ('hsvrid', 'int'), ('status', 'int'),  \
    ('eventid', 'int'), ('event_type', 'int'), ('assoc_lvolid', 'int')], \
    'primary_key': 'lvolid'} 
IDTABLE_DEF = {'name': 'idtable', 'fields': [('type', 'int'), ('id', 'int'), \
    ('maxid', 'int'), ('rotate', 'int')], \
    'primary_key': 'type'} 

class Db_table:
    def __init__(self, table_def, db):
        self.db_name = table_def['name']
        self.primary_key = table_def['primary_key']
        keys = []
        pythontypes = []
        insert_str1 = "insert into %s (" % self.db_name
        insert_str2 = " values("
        create_str = "create table %s (" % self.db_name
        comma = ""
        for key, type in table_def['fields']:
            keys.append(key)
            insert_str1 += "%s%s" % (comma, key)
            if type == 'int':
                insert_str2 += "%s%%d" % (comma)
                pythontypes.append(types.IntType)
            else:
                insert_str2 += "%s%%s" % (comma)
                pythontypes.append(types.StringType)
            if key == table_def['primary_key']:
                type += " primary key"
            create_str += "%s%s %s" % (comma, key, type)
            comma = ", "
        self.db_keys = keys
        self.db_pythontypes = pythontypes
        self.sql_create_str = create_str + ")"
        self.sql_insert_str = insert_str1 + ")" + insert_str2 + ")"
        self.db = db
        db.add_table(self)

    def initialize_table(self):
        self.db.c.execute(self.sql_create_str)

    def put_row(self, args):
        assert(len(args) == len(self.db_pythontypes))
        for v, t in zip(args, self.db_pythontypes):
            assert(type(v) == t)
        self.db.c.execute(self.sql_insert_str, args)

    def delete_rows(self, key, value):
        self.db.c.execute("delete from %s where %s = %r" % (self.db_name, key, value))

    def get_row(self, value):
        self.db.c.execute("select * from %s where %s = %r" % (self.db_name, self.primary_key, value))
        row = self.db.c.fetchone()
        if row:
            assert(len(row) == len(self.db_pythontypes))
            for v, t in zip(row, self.db_pythontypes):
                assert(type(v) == t)
            return dict(zip(self.db_keys, row))
        else:
            return None

    def get_rows(self, key, value):
        retval = []
        if value == '*':
            self.db.c.execute("select * from %s where %s" % (self.db_name, key))
        else:
            self.db.c.execute("select * from %s where %s = %r" % (self.db_name, key, value))
        rows = self.db.c.fetchall()
        for row in rows:
            assert(len(row) == len(self.db_pythontypes))
            for v, t in zip(row, self.db_pythontypes):
                assert(type(v) == t)
            retval.append(dict(zip(self.db_keys, row)))
        return retval

    def update_value(self, key, value, target, newvalue):
        self.db.c.execute("update %s set %s = %s where %s = %r" % (self.db_name, target, newvalue, key, value))

    def rowcount(self):
        sql = "select * from %s" % (self.db_name)
        self.db.c.execute(sql)
        return(self.db.c.rowcount)

class Db_attach(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, ATTACH_DEF, db)

    def set_status(self, lvolid, status):
        self.update_value('lvolid', lvolid, 'status', status)

    def start_event(self, eventid, lvolid, assoc_lvolid, type):
        self.update_value('lvolid', lvolid, 'eventid', eventid)
        self.update_value('lvolid', lvolid, 'event_type', type)
        if lvolid != assoc_lvolid:
            self.update_value('lvolid', lvolid, 'assoc_lvolid', assoc_lvolid)

    def __clear_event(self, lvolid):
        self.update_value('lvolid', lvolid, 'eventid', 0)
        self.update_value('lvolid', lvolid, 'event_type', 0)
        self.update_value('lvolid', lvolid, 'assoc_lvolid', 0)

    def end_event_success(self, eventid, lvolid):
        attach = self.get_row(lvolid)
        assert(attach and attach['eventid'] == eventid), lineno()
        if attach['event_type'] == ATTACH_EVENT['UNBINDING'] and \
            not attach['assoc_lvolid']:
            self.delete_rows('lvolid', lvolid)
        else:
            self.__clear_event(lvolid)

    def end_event_cancel(self, eventid, lvolid):
        attach = self.get_row(lvolid)
        assert(attach and attach['eventid'] == eventid), lineno()
        if attach['event_type'] == ATTACH_EVENT['BINDING'] and \
            not attach['assoc_lvolid']:
            self.delete_rows('lvolid', lvolid)
        else:
            self.__clear_event(lvolid)

    def end_event_error(self, eventid, lvolid):
        attach = self.get_row(lvolid)
        assert(attach and attach['eventid'] == eventid), lineno()
        self.update_value('lvolid', lvolid, 'status', ATTACH_STATUS['ERROR'])
        self.__clear_event(lvolid)

class Db_ipdata(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, IPDATA_DEF, db)

    def __get_ipdata(self, key, id):
        rows = self.get_rows(key, id)
        retval = []
        for row in rows:
            retval.append(row['ip'])
        return retval

    def ssvr_ipdata(self, ssvrid):
        return self.__get_ipdata('ssvrid', ssvrid)

    def hsvr_ipdata(self, hsvrid):
        return self.__get_ipdata('hsvrid', hsvrid)

    def ipdata_to_svr(self, ipdata):
        hsvr = []
        ssvr = []
        for ip in ipdata:
            row = self.get_row(ip)
            if row:
                if row['hsvrid'] != 0:
                    hsvr.append(row['hsvrid'])
                if row['ssvrid'] != 0:
                    ssvr.append(row['ssvrid'])
        return hsvr, ssvr

    def __unique_id(self, ids):
        for id in ids:
            if id != ids[0]:
                return False
        return True

    def hsvr_match(self, ipdata):
        hsvr, _ = self.ipdata_to_svr(ipdata)
        if len(hsvr) == 0:
            return 0
        if len(hsvr) != len(ipdata) or not self.__unique_id(hsvr):
            raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
        return hsvr[0]

    def ssvr_match(self, ipdata):
        _, ssvr = self.ipdata_to_svr(ipdata)
        if len(ssvr) == 0:
            return 0
        if len(ssvr) != len(ipdata) or not self.__unique_id(ssvr):
            raise xmlrpclib.Fault(errno.EEXIST, 'EEXIST')
        return ssvr[0]

    def put_hsvr_ipdata(self, hsvrid, ipdata):
        for ip in ipdata:
            row = self.get_row(ip)
            if row:
                # assert row.hsvrid == 0
                self.update_value('ip', ip, 'hsvrid', hsvrid)
            else:
                self.put_row((ip, hsvrid, 0))

    def put_ssvr_ipdata(self, ssvrid, ipdata):
        for ip in ipdata:
            row = self.get_row(ip)
            if row:
                # assert row.ssvrid == 0
                self.update_value('ip', ip, 'ssvrid', ssvrid)
            else:
                self.put_row((ip, 0, ssvrid))

    def delete_ssvr(self, ssvrid):
        rows = self.get_rows('ssvrid', ssvrid)
        for r in rows:
            if r['hsvrid'] != 0:
                self.update_value('ip', r['ip'], 'ssvrid', 0)
            else:
                self.delete_rows('ip', r['ip'])

    def delete_hsvr(self, hsvrid):
        rows = self.get_rows('hsvrid', hsvrid)
        for r in rows:
            if r['ssvrid'] != 0:
                self.update_value('ip', r['ip'], 'hsvrid', 0)
            else:
                self.delete_rows('ip', r['ip'])
            
class Db_ssvrlst(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, SSVRLST_DEF, db)

class Db_hsvrlst(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, HSVRLST_DEF, db)

class Db_pdsklst(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, PDSKLST_DEF, db)

class Db_lvollst(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, LVOLLST_DEF, db)

class Db_dskmap(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, DSKMAP_DEF, db)

    def get_extent(self, pdskid, offset):
        try:
            sql = "select * from %s where pdskid = %d and offset <= %d and offset+capacity > %d" % (self.db_name, pdskid, offset, offset)
            self.db.c.execute(sql)

            row = self.db.c.fetchone()
            if row:
                return dict(zip(self.db_keys, row))
            else:
                return None
        except Exception:
            logger.error(traceback.format_exc())
            return None

class Db_lvolmap(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, LVOLMAP_DEF, db)

    def get_rows(self, key, value):     # order by offset
        retval = []
        self.db.c.execute("select * from %s where %s = %r order by offset" % (self.db_name, key, value))
        rows = self.db.c.fetchall()
        for row in rows:
            retval.append(dict(zip(self.db_keys, row)))
        return retval

class Db_resync(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, RESYNC_DEF, db)

class Db_shred(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, SHRED_DEF, db)

class Db_idtable(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, IDTABLE_DEF, db)

    def init_id(self, type, maxid, rotate):
        self.put_row((type, 1, maxid, rotate))

    def genid(self, type):
        row = self.get_row(type)
        id = row['id']
        if id == row['maxid']:
            if row['rotate']:
                self.update_value('type', type, 'id', 1)
                return 1
            else:
                raise xmlrpclib.Fault(errno.ENOSPC, 'ENOSPC')
        self.update_value('type', type, 'id', id + 1)
        return id

class Db_assoc(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, ASSOC_DEF, db)

class Db_snapshot(Db_table):
    def __init__(self, db):
        Db_table.__init__(self, SNAPSHOT_DEF, db)

class Db_components(Db_base):
    def __init__(self, path):
        Db_base.__init__(self, path)
        self.resync = Db_resync(self)
        self.shred = Db_shred(self)
        self.lvollst = Db_lvollst(self)
        self.attach = Db_attach(self)
        self.ipdata = Db_ipdata(self)
        self.ssvrlst = Db_ssvrlst(self)
        self.hsvrlst = Db_hsvrlst(self)
        self.pdsklst = Db_pdsklst(self)
        self.dskmap = Db_dskmap(self)
        self.lvolmap = Db_lvolmap(self)
        self.idtable = Db_idtable(self)
        self.assoc = Db_assoc(self)
        self.snapshot = Db_snapshot(self)

    def initialize_tables(self):
        Db_base.initialize_tables(self)
        self.__create_view_dskspace()
        for type in (IDTYPE['hsvr'], IDTYPE['lvol'], IDTYPE['pdsk'], IDTYPE['ssvr']):
            self.idtable.init_id(type, MAX_RESOURCEID, 0)
        self.idtable.init_id(IDTYPE['event'], MAX_EVENTID, 1)
        self.commit()

    def genid_hsvr(self):
        return self.idtable.genid(IDTYPE['hsvr'])

    def genid_lvol(self):
        return self.idtable.genid(IDTYPE['lvol'])

    def genid_pdsk(self):
        return self.idtable.genid(IDTYPE['pdsk'])

    def genid_ssvr(self):
        return self.idtable.genid(IDTYPE['ssvr'])

    def genid_event(self):
        return self.idtable.genid(IDTYPE['event'])

    def __create_view_dskspace(self):
        sql = """
create view dskspace as
    select
        a.pdskid as pdskid,
        pdsklst.ssvrid as ssvrid,
        available,
        pdsklst.capacity as total,
        (available * 100 / pdsklst.capacity) as percent,
        pdsklst.priority as priority
    from
        (select pdskid, sum(capacity) as available
         from dskmap
         where status = %s
         group by pdskid) a,
        pdsklst,
        ssvrlst
    where
        a.pdskid = pdsklst.pdskid and
        pdsklst.ssvrid = ssvrlst.ssvrid and
        (pdsklst.priority = %s or pdsklst.priority = %s) and
        (ssvrlst.priority = %s or ssvrlst.priority = %s)
        """ % (EXT_STATUS['FREE'], \
        ALLOC_PRIORITY['HIGH'], ALLOC_PRIORITY['LOW'], \
        ALLOC_PRIORITY['HIGH'], ALLOC_PRIORITY['LOW'])
        self.c.execute(sql)
        self.commit()

    def rebuildfinished_mirror(self, dextid):
        self.lvolmap.update_value('lvolid', dextid, 'mirror_status', MIRROR_STATUS['INSYNC'])

    def allocate_dext(self, capacity, omit):
        sqlstr = ""
        for id in omit:
            dskmap = self.dskmap.get_row(id)
            dsk = self.pdsklst.get_row(dskmap['pdskid'])
            sqlstr += "and ssvrid != %s " % dsk['ssvrid']

        # 1) select the storage server of higher free space other than omit
        # 2) select the disk of higher free space from the storage server
        sql = """
select 
    dskspace.pdskid,dskspace.ssvrid
from 
    (select ssvrid, max(percent) as mp, avg(percent) as avg 
     from dskspace 
     where available >= %s %s
     group by ssvrid order by avg desc, mp desc limit 1) a, 
    dskspace 
where 
    a.ssvrid=dskspace.ssvrid and 
    a.mp=dskspace.percent
        """ % (capacity, sqlstr)
        self.c.execute(sql)
        if self.c.rowcount < 1:
            # ENOSPC
            raise Exception, "allocate_dext: no free space"
        pdskid, ssvrid = self.c.fetchone()

        # select the largest free extent
        sql = """
select 
    dextid, offset, capacity 
from 
    dskmap 
where 
    status = %s and pdskid = %s 
order by 
    capacity desc
        """ % (EXT_STATUS['FREE'], pdskid)
        self.c.execute(sql)
        if self.c.rowcount < 1:
            # ENOSPC
            raise Exception, "allocate_dext: no free space"
        dextid, offset, size = self.c.fetchone()
        if size > capacity:
            # divide the extent before use
            self.dskmap.update_value('dextid', dextid, 'capacity', capacity)
            self.dskmap.update_value('dextid', dextid, 'status', EXT_STATUS['BUSY'])

            freeid = self.genid_lvol()
            self.dskmap.put_row(((pdskid, freeid, offset + capacity, size - capacity, EXT_STATUS['FREE'])))
        elif size == capacity:
            # use the extent as it is
            self.dskmap.update_value('dextid', dextid, 'status', EXT_STATUS['BUSY'])
        else:
            # ENOSPC
            raise Exception, "allocate_dext: no free space"

        return dextid

    # delete old lvolmap, modify old dskmap
    # allocate new dext, add lvolmap
    # add resync (if attached)
    def replace_dext(self, dextid, post_status):
        # delete lvolmap first, get values before delete
        lvolmap = self.lvolmap.get_row(dextid)
        self.lvolmap.delete_rows('lvolid', dextid)
        # change dskmap status if specified (OFFLINE or FAULTY)
        if post_status:
            self.dskmap.update_value('dextid', dextid, 'status', post_status)
        # allocate new dext
        mirrorid = lvolmap['superlvolid']
        extents = self.lvolmap.get_rows('superlvolid', mirrorid)
        omit = []
        for extent in extents:
            omit.append(extent['lvolid'])
        newdextid = self.allocate_dext(lvolmap['capacity'], omit)
        if lvolmap['mirror_status'] in (MIRROR_STATUS['ALLOCATED'], MIRROR_STATUS['NEEDSHRED']):
            ms = MIRROR_STATUS['ALLOCATED']
        else:
            ms = MIRROR_STATUS['SPARE']
        self.lvolmap.put_row((newdextid, LVOLTYPE['DEXT'], mirrorid, 0, lvolmap['capacity'], \
            lvolmap['toplvolid'], ms))
        # register resync table if attached
        attach = self.attach.get_row(lvolmap['toplvolid'])
        if attach:
            # TODO? check unbinding?
            merge = False
            row = self.resync.get_rows('lvolid_add', dextid)
            if row:
                assert(len(row) == 1), lineno()
                if row[0]['status'] == EVENT_STATUS['PENDING']:
                    merge = True
                else: # PROGRESS
                    # delete resync record so that not retry
                    self.resync.delete_rows('lvolid_add', dextid)
            if merge:
                self.resync.update_value('lvolid_add', dextid, 'lvolid_add', newdextid)
            else:
                eventid = self.genid_event()
                self.resync.put_row((eventid, mirrorid, newdextid, dextid, EVENT_STATUS['PENDING']))

    def get_dskspace(self):
        result = {}
        ssvr = {}
        dsk = {}
        self.c.execute("select ssvrid, sum(available) from dskspace group by ssvrid")
        for ssvrid, available in self.c.fetchall():
            assert(type(ssvrid) == types.IntType)
            assert(type(available) == types.IntType)
            ssvr[ssvrid] = available
        self.c.execute("select ssvrid, sum(capacity) from pdsklst group by ssvrid")
        for ssvrid, total in self.c.fetchall():
            assert(type(ssvrid) == types.IntType)
            assert(type(total) == types.IntType)
            if ssvr.has_key(ssvrid):
                s = ssvr[ssvrid]
                av = s
            else:
                av = 0
            ssvr[ssvrid] = [av, total]
            
        result['ssvr'] = ssvr
        
        self.c.execute("select pdskid, available from dskspace")
        for pdskid, available in self.c.fetchall():
            dsk[pdskid] = available
        result['pdsk'] = dsk

        return result

def main():
    # initialize DB
    try:
        db = Db_components(DB_COMPONENTS)
        db.connect()
        db.initialize_tables()
        db.disconnect()
    except Exception, inst:
        print >> sys.stderr, "DB initialization failed. %s" % (inst)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
