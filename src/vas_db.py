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

__version__ = '$Id: vas_db.py 95 2010-07-16 01:14:35Z yamamoto2 $'

import sys
import sqlite3 as sqlite
import traceback
from vas_conf import DB_COMPONENTS, DB_EVENTS, EXTENTSIZE, \
logger, MAX_EVENTID, MAX_LENGTH, MAX_REDUNDANCY, MIN_REDUNDANCY
from vas_subr import lineno, get_lvolstruct_of_lvoltype

def __reverse_dict(a):
    return dict(zip(a.values(), a.keys()))

ALLOC_PRIORITY = {'HIGH':1, 'LOW':2, 'EVACUATE':3, 'OFFLINE':4, 'FAULTY':5, 'HALT':6}
ALLOC_PRIORITY_STR = __reverse_dict(ALLOC_PRIORITY)
EXT_STATUS = {'BUSY':1, 'FREE':2, 'EVACUATE':3, 'OFFLINE':4, 'SUPER':5, 'FAULTY':6}
BIND_STATUS = {'BOUND':1, 'ALLOCATED':2, 'UNBOUND':3, 'BINDING':4, 'UNBINDING':5}
MIRROR_STATUS = {'DEFAULT':1, 'VALID':2, 'INVALID':3}
MIRROR_STATUS_STR = __reverse_dict(MIRROR_STATUS)
EVENT_STATUS = {'OPEN':1, 'CLOSED':2, 'PENDING':3, 'PROGRESS':4}
EVENT_STATUS_STR = __reverse_dict(EVENT_STATUS)
LVOLTYPE = {'LINEAR':1, 'MIRROR':2, 'DEXT':3}
TARGET = {'HSVR':1, 'SSVR':2, 'PDSK': 3, 'LVOL':4}
TARGET_PREFIX = ['none-', 'hsvr-', 'ssvr-', 'pdsk-', 'lvol-']
EVENT = {'ABNORMAL':1, 'RESYNC':2, 'SHRED': 3}
EVENT_STR = __reverse_dict(EVENT)
DB_CONNECTIONS = {}

class Db_base:
    conn = None
    c = None

    def __init__(self):
        pass

    def connect(self, path):
        if self.conn:
            return
        if path in DB_CONNECTIONS:
            conn_holder = DB_CONNECTIONS[path]
            conn_holder['refcount'] += 1
        else:
            conn_holder = {'conn': sqlite.connect(path, check_same_thread = False),'refcount': 1}
            DB_CONNECTIONS[path] = conn_holder
        self.conn = conn_holder['conn']
        self.c = self.conn.cursor()

    def disconnect(self):
        if not self.conn:
            return
        conn_holder = {}
        for path in DB_CONNECTIONS:
            conn_holder = DB_CONNECTIONS[path]
            if self.conn == conn_holder['conn']:
                conn_holder['refcount'] -= 1
                if conn_holder['refcount'] == 0:
                    self.c.close()
                    self.conn.close()
                    del DB_CONNECTIONS[path]
                break
        self.conn = None

    def initialize_tables(self):
        pass

    def begin_transaction(self):
        #assert(self.conn.inTransaction == 0), lineno()
        True

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

SSVRLST_DEF = {'name': 'ssvrlst', 'fields': [('ssvrid', 'int'), ('priority', 'int'), \
    ('ip_data_1', 'varchar(20)'), ('resync', 'int'), ('latency', 'int'), ('ip_data_2', 'varchar(20)')], \
    'primary_key': 'ssvrid'}
HSVRLST_DEF = {'name': 'hsvrlst', 'fields': [('hsvrid', 'int'), ('priority', 'int'), \
    ('ip_data_1', 'varchar(20)'), ('resync', 'int'), ('latency', 'int'), ('ip_data_2', 'varchar(20)')], \
    'primary_key': 'hsvrid'}
PDSKLST_DEF = {'name': 'pdsklst', 'fields': [('ssvrid', 'int'), ('pdskid', 'int'), \
    ('capacity', 'int'), ('iscsi_path_1', 'varchar(1024)'), ('iscsi_path_2', 'varchar(1024)'), \
    ('srp_name', 'varchar(1024)'), ('local_path', 'varchar(1024)'), ('priority', 'int'), ('resync', 'int')], \
    'primary_key': 'pdskid'}
LVOLLST_DEF = {'name': 'lvollst', 'fields': [('hsvrid', 'int'), ('lvolid', 'int'), \
    ('lvolname', 'varchar(64)'), ('redundancy', 'int'), ('capacity', 'int')], \
    'primary_key': 'lvolid'}
DSKMAP_DEF = {'name': 'dskmap', 'fields': [('pdskid', 'int'), ('dextid', 'int'), \
    ('offset', 'int'), ('capacity', 'int'), ('status', 'int')], \
    'primary_key': 'dextid'}
LVOLMAP_DEF = {'name': 'lvolmap', 'fields': [('lvolid', 'int'), ('lvoltype', 'int'), \
    ('superlvolid', 'int'), ('offset', 'int'), ('capacity', 'int'), ('status', 'int'), ('mirror', 'int')], \
    'primary_key': 'lvolid'}
TRANSIT_DEF = {'name': 'transit', 'fields': [('eventid', 'int'), ('target', 'int'), \
    ('id', 'int'), ('event', 'int'), ('status', 'int')], \
    'primary_key': 'eventid'}
RESYNC_DEF = {'name': 'resync', 'fields': [('eventid', 'int'), ('mirrorid', 'int'), \
    ('lvolid_add', 'int'), ('lvolid_rm', 'int'), ('ssvrid_add', 'int'), ('ssvrid_rm', 'int'), \
    ('iscsi_path_1_add', 'varchar(1024)'), ('iscsi_path_1_rm', 'varchar(1024)'), \
    ('iscsi_path_2_add', 'varchar(1024)'), ('iscsi_path_2_rm', 'varchar(1024)'), \
    ('capacity', 'int'), ('offset', 'int')], \
    'primary_key': 'eventid'}
SHRED_DEF = {'name': 'shred', 'fields': [('eventid', 'int'), ('lvolid', 'int'), ('offset', 'int'), \
    ('capacity', 'int'), ('pdskid', 'int')], \
    'primary_key': 'eventid'}

class Db_common(Db_base):
    def __init__(self, table_def, db_file):
        self.db_name = table_def['name']
        self.primary_key = table_def['primary_key']
        keys = []
        insert_str1 = "insert into %s (" % self.db_name
        insert_str2 = " values("
        create_str = "create table %s (" % self.db_name
        comma = ""
        for key, type in table_def['fields']:
            keys.append(key)
            insert_str1 += "%s%s" % (comma, key)
            if type == 'int':
                insert_str2 += "%s%%d" % (comma)
            else:
                insert_str2 += "%s'%%s'" % (comma)
            if key == table_def['primary_key']:
                type += " primary key"
            create_str += "%s%s %s" % (comma, key, type)
            comma = ", "
        self.db_keys = keys
        self.sql_create_str = create_str + ")"
        self.sql_insert_str = insert_str1 + ")" + insert_str2 + ")"
        self.connect(db_file)

    def initialize_tables(self):
        try:
            self.c.execute(self.sql_create_str)
        except Exception:
            logger.error(traceback.format_exc())
        try:
            self.commit()
        except sqlite.DatabaseError:
            self.rollback()

    def put_row(self, args):
        self.c.execute(self.sql_insert_str % args)

    def delete_rows(self, key, value):
        self.c.execute("delete from %s where %s = %r" % (self.db_name, key, value))

    def get_row(self, value):
        self.c.execute("select * from %s where %s = %r" % (self.db_name, self.primary_key, value))
        row = self.c.fetchone()
        if row:
            return dict(zip(self.db_keys, row))
        else:
            return None

    def get_rows(self, key, value):
        retval = []
        if value == '*':
            self.c.execute("select * from %s where %s" % (self.db_name, key))
        else:
            self.c.execute("select * from %s where %s = %r" % (self.db_name, key, value))
        rows = self.c.fetchall()
        for row in rows:
            retval.append(dict(zip(self.db_keys, row)))
        return retval

    def update_value(self, key, value, target, newvalue):
        self.c.execute("update %s set %s = %s where %s = %r" % (self.db_name, target, newvalue, key, value))

    def rowcount(self):
        sql = "select * from %s" % (self.db_name)
        self.c.execute(sql)
        return( len( self.c.fetchall() ) )

    def get_svr_by_ip_data(self, ip_data):
        self.c.execute("select * from %s where ip_data_1 in (%r, %r) or ip_data_2 in (%r, %r)" % (self.db_name, ip_data[0], ip_data[1], ip_data[0], ip_data[1]))
        result = self.c.fetchall()
        if len(result) > 1:
            raise Exception, "IP address confliction"
        if len(result) == 0:
            return None
        row = dict(zip(self.db_keys, result[0]))
        if row['ip_data_1'] == ip_data[0] and row['ip_data_2'] == ip_data[1]:
            return row
        else:
            raise Exception, "IP address confliction"

class Db_ssvrlst(Db_common):
    def __init__(self):
        Db_common.__init__(self, SSVRLST_DEF, DB_COMPONENTS)

    def get_resync_load(self, id):
        row = self.get_row(id)
        return row['resync']

class Db_hsvrlst(Db_common):
    def __init__(self):
        Db_common.__init__(self, HSVRLST_DEF, DB_COMPONENTS)

    def get_resync_load(self, id):
        row = self.get_row(id)
        return row['resync']

class Db_pdsklst(Db_common):
    def __init__(self):
        Db_common.__init__(self, PDSKLST_DEF, DB_COMPONENTS)

    def get_resync_load(self, id):
        row = self.get_row(id)
        return row['resync']

class Db_lvollst(Db_common):
    def __init__(self):
        Db_common.__init__(self, LVOLLST_DEF, DB_COMPONENTS)

class Db_dskmap(Db_common):
    def __init__(self):
        Db_common.__init__(self, DSKMAP_DEF, DB_COMPONENTS)

    def get_extent(self, pdskid, offset):
        try:
            sql = "select * from %s where pdskid = %d and offset <= %d and offset+capacity > %d" % (self.db_name, pdskid, offset, offset)
            self.c.execute(sql)

            row = self.c.fetchone()
            if row:
                return dict(zip(self.db_keys, row))
            else:
                return None
        except Exception:
            logger.error(traceback.format_exc())
            return None

    def get_dextids_evacuate(self, pdskid):
        dextids = []
        sql = "select dextid from %s where pdskid = %d and status = %d" % (self.db_name, pdskid, EXT_STATUS['EVACUATE'])
        self.c.execute(sql)

        rows = self.c.fetchall()
        for row in rows:
            dextids.append(row[0])
        return dextids

class Db_lvolmap(Db_common):
    def __init__(self):
        Db_common.__init__(self, LVOLMAP_DEF, DB_COMPONENTS)

    def get_rows_order_by_offset(self, key, value):
        retval = []
        self.c.execute("select * from %s where %s = %r order by offset" % (self.db_name, key, value))
        rows = self.c.fetchall()
        for row in rows:
            retval.append(dict(zip(self.db_keys, row)))
        return retval

    def count_valid_copy(self, dextid):
        lvolmap_row = self.get_row(dextid)
        assert(lvolmap_row['lvoltype'] == LVOLTYPE['DEXT']), lineno()
        sql = "select * from %s where superlvolid = %d and mirror = %d" % (self.db_name, lvolmap_row['superlvolid'], MIRROR_STATUS['VALID'])
        self.c.execute(sql)
        return( len( self.c.fetchall() ) )

class Db_transit(Db_common):
    def __init__(self):
        Db_common.__init__(self, TRANSIT_DEF, DB_EVENTS)

    def get_resyncs(self, status):
        retval = []
        sql = "select * from %s where event = %d and status = %d" % (self.db_name, EVENT['RESYNC'], status)
        self.c.execute(sql)

        rows = self.c.fetchall()
        for row in rows:
            retval.append(dict(zip(self.db_keys, row)))
        return retval

    def count_progress_resyncs(self):
        sql = "select * from %s where event = %d and status = %d" % (self.db_name, EVENT['RESYNC'], EVENT_STATUS['PROGRESS'])
        self.c.execute(sql)
        return( len( self.c.fetchall() ) )

    def get_shreds(self, status):
        retval = []
        sql = "select * from %s where event = %d and status = %d" % (self.db_name, EVENT['SHRED'], status)
        self.c.execute(sql)

        rows = self.c.fetchall()
        for row in rows:
            retval.append(dict(zip(self.db_keys, row)))
        return retval

    def count_progress_shreds(self):
        sql = "select * from %s where event = %d and status = %d" % (self.db_name, EVENT['SHRED'], EVENT_STATUS['PROGRESS'])
        self.c.execute(sql)
        return( len( self.c.fetchall() ) )

class Db_resync(Db_common):
    def __init__(self):
        Db_common.__init__(self, RESYNC_DEF, DB_EVENTS)

    def merge_row(self, row):
        sql = "update %s set lvolid_rm = %d, ssvrid_rm = %d, iscsi_path_1_rm = \"%s\", iscsi_path_2_rm = \"%s\" where lvolid_rm = %d" % (self.db_name, row['lvolid_rm'], row['ssvrid_rm'], row['iscsi_path_1_rm'], row['iscsi_path_2_rm'], row['lvolid_add'])
        self.c.execute(sql)

class Db_shred(Db_common):
    def __init__(self):
        Db_common.__init__(self, SHRED_DEF, DB_EVENTS)

class LogicalVolume(Db_base):
    def __init__(self):
        self.db_name = 'dskspace'
        self.connect(DB_COMPONENTS)
        self.dskmap = Db_dskmap()
        self.pdsklst = Db_pdsklst()
        self.lvolmap = Db_lvolmap()
        self.lvollst = Db_lvollst()
        self.genid_lvol = Db_genid_lvol()

    def disconnect(self):
        Db_base.disconnect(self)
        self.dskmap.disconnect()
        self.pdsklst.disconnect()
        self.lvolmap.disconnect()
        self.lvollst.disconnect()
        self.genid_lvol.disconnect()

    def initialize_tables(self):
        sql = """
create view dskspace as
    select
        a.pdskid as pdskid,
        pdsklst.ssvrid as ssvrid,
        available,
        pdsklst.capacity as total,
        (available * 100 / pdsklst.capacity) as percent,
        iscsi_path_1,
        iscsi_path_2,
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

    def __allocate_free_dexts(self, extlen, redundancy, lvvolid, lvoffset):
        mirrorid = self.genid_lvol.genid()
        if not mirrorid:
            # ENOSPC
            raise Exception, "__allocate_free_dexts: can not get new mirrorid"

        # 1) select top #redundancy of storage servers at average free disk(HIGH) space ratio
        # 2) select top 1 disks(HIGH) from the each storage servers
        # note: at least #redundancy storage servers required
        sql = """
select 
    dskspace.pdskid,dskspace.ssvrid
from 
    (select ssvrid, max(percent) as mp, avg(percent) as avg 
     from dskspace 
     where available >= %s and priority = %s
     group by ssvrid order by avg desc, mp desc limit %s) a, 
    dskspace 
where 
    a.ssvrid=dskspace.ssvrid and 
    dskspace.priority = %s and
    a.mp=dskspace.percent
        """ % (extlen, ALLOC_PRIORITY['HIGH'], redundancy, ALLOC_PRIORITY['HIGH'])
        self.c.execute(sql)
        res = self.c.fetchall()

        allocated = {}

        for pdskid, ssvrid in res:
            # not select multiple disks on a single storage server 
            if not allocated.has_key(ssvrid):
                # select the largest free extent
                sql = """
select 
    pdskid,
    dextid,
    offset,
    capacity 
from 
    dskmap 
where 
    status = %s and 
    pdskid = %s 
order by 
    capacity desc
                """ % (EXT_STATUS['FREE'], pdskid)
                self.c.execute(sql)
                pdskid, lvolid, offset, l = self.c.fetchone()
                if l > extlen:
                    allocated[ssvrid] = True
                    # divide the extent before use
                    self.dskmap.update_value('dextid', lvolid, 'capacity', extlen)
                    self.dskmap.update_value('dextid', lvolid, 'status', EXT_STATUS['BUSY'])
                    self.lvolmap.put_row(((lvolid, LVOLTYPE['DEXT'], mirrorid, 0, extlen, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['VALID'])))

                    freeid = self.genid_lvol.genid()
                    if not freeid:
                        # ENOSPC
                        raise Exception, "__allocate_free_dexts: can not get new dextid"
                    self.dskmap.put_row(((pdskid, freeid, offset + extlen, l - extlen, EXT_STATUS['FREE'])))
                elif l == extlen:
                    allocated[ssvrid] = True
                    # use the extent as it is
                    self.dskmap.update_value('dextid', lvolid, 'status', EXT_STATUS['BUSY'])
                    self.lvolmap.put_row(((lvolid, LVOLTYPE['DEXT'], mirrorid, 0, extlen, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['VALID'])))

        if len(allocated) < redundancy:
            # ENOSPC
            raise Exception, "__allocate_free_dexts: can not allocate %d disk extents on separate storage servers" % redundancy

        self.lvolmap.put_row(((mirrorid, LVOLTYPE['MIRROR'], lvvolid, lvoffset, extlen, BIND_STATUS['ALLOCATED'], 0)))
        return

    def create(self, lvolname, redundancy, capacity, dminfo):
        allocate = capacity

        reside = allocate

        lvoffset = 0

        # create new lvvolod
        lvvolid = self.genid_lvol.genid()
        if not lvvolid:
            # ENOSPC
            raise Exception, "create: can not get new lvolid"

        for extlen in EXTENTSIZE:
            while reside / extlen > 0:
                self.__allocate_free_dexts(extlen, redundancy, lvvolid, lvoffset)
                reside -= extlen
                lvoffset += extlen

        self.lvolmap.put_row(((lvvolid, LVOLTYPE['LINEAR'], 0, 0, allocate, BIND_STATUS['ALLOCATED'], 0)))

        self.lvollst.put_row(((0, lvvolid, lvolname, redundancy, capacity)))

        return lvvolid

    def remove(self, lvvolid):

        def get_dskmap_status(lvolid_dext):
            dskmap = self.dskmap.get_row(lvolid_dext)
            return dskmap['status']

        def set_dskmap_status(lvolid_dext, status):
            self.dskmap.update_value('dextid', lvolid_dext, 'status', status)

        offlined_dexts = []

        lvolstruct = self.get_lvolstruct(lvvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])

        if lvolstruct_linear['lvolspec']['hsvrid'] != 0:
            raise Exception, "remove: logical volume %08x is attach" % lvolstruct_linear['lvolid']

        assert(lvolstruct_linear['bind_status'] != BIND_STATUS['BOUND']), lineno()

        for mirror in lvolstruct_linear['components']:
            assert(mirror['bind_status'] != BIND_STATUS['BOUND']), lineno()
            
            for extent in mirror['components']:
                lvolid_dext = extent['lvolid']

                assert(extent['bind_status'] != BIND_STATUS['BOUND']), lineno()

                assert(get_dskmap_status(lvolid_dext) != EXT_STATUS['EVACUATE']), lineno()

                self.lvolmap.delete_rows('lvolid', lvolid_dext)

                if extent['bind_status'] == BIND_STATUS['ALLOCATED']:
                    set_dskmap_status(lvolid_dext, EXT_STATUS['FREE'])
                    # ommit zero-clearing for unused disk extents
                    continue
                else:
                    set_dskmap_status(lvolid_dext, EXT_STATUS['OFFLINE'])
                offlined_dexts += [lvolid_dext]

            self.lvolmap.delete_rows('lvolid', mirror['lvolid'])

        self.lvolmap.delete_rows('lvolid', lvvolid)
        self.lvollst.delete_rows('lvolid', lvvolid)

        return offlined_dexts

    def get_lvolstruct(self, linear_lvolid):

        def __get_capacity(linear_lvolid):
            lvollst = self.lvollst.get_row(linear_lvolid)
            return lvollst['capacity']

        def __get_components_dext(mirror_lvolid):

            def __get_lvolspec_dext(dext_lvolid):
                dskmap = self.dskmap.get_row(dext_lvolid)
                pdsklst = self.pdsklst.get_row(dskmap['pdskid'])
                return {'pdskid': dskmap['pdskid'], 'offset': dskmap['offset'], \
                'ssvrid': pdsklst['ssvrid'], 'iscsi_path': (pdsklst['iscsi_path_1'], \
                pdsklst['iscsi_path_2'])}

            extents = self.lvolmap.get_rows('superlvolid', mirror_lvolid)
            assert len(extents), lineno()

            components = []
            for extent in extents:

                lvolstruct = {'lvolid': extent['lvolid'], 'lvoltype': LVOLTYPE['DEXT'], \
                'capacity': extent['capacity'], 'bind_status': extent['status']}

                lvolspec = __get_lvolspec_dext(lvolstruct['lvolid'])
                lvolspec['status'] = extent['mirror']

                lvolstruct['lvolspec'] = lvolspec
                lvolstruct['components'] = []

                components.append(lvolstruct)

            return components

        def __get_components_mirror(linear_lvolid):
            mirrors = self.lvolmap.get_rows_order_by_offset('superlvolid', linear_lvolid)
            assert len(mirrors), lineno()

            components = []
            for mirror in mirrors:

                lvolstruct = {'lvolid': mirror['lvolid'], 'lvoltype': LVOLTYPE['MIRROR'], \
                'capacity': mirror['capacity'], 'bind_status': mirror['status'], \
                'lvolspec': {'add': [], 'remove': []}}

                lvolstruct['components'] = __get_components_dext(lvolstruct['lvolid'])

                components.append(lvolstruct)

            return components


        def __get_component_linear(linear_lvolid):
            linear = self.lvolmap.get_row(linear_lvolid)
            assert linear, lineno()

            lvolstruct = {'lvolid': linear['lvolid'], 'lvoltype': LVOLTYPE['LINEAR'], \
            'capacity': linear['capacity'], 'bind_status': linear['status'] }
            
            lvollst = self.lvollst.get_row(lvolstruct['lvolid'])
            lvolspec = {'lvolname': lvollst['lvolname'], 'hsvrid': lvollst['hsvrid']}

            lvolstruct['lvolspec'] = lvolspec
            lvolstruct['components'] = __get_components_mirror(lvolstruct['lvolid'])

            return lvolstruct

        return __get_component_linear(linear_lvolid)

    def __set_bindstatus(self, lvolstruct_linear, status):
        self.lvolmap.update_value('lvolid', lvolstruct_linear['lvolid'], 'status', status)
        for mirror in lvolstruct_linear['components']:
            self.lvolmap.update_value('lvolid', mirror['lvolid'], 'status', status)
            for extent in mirror['components']:
                self.lvolmap.update_value('lvolid', extent['lvolid'], 'status', status)

    def binding(self, lvvolid, hsvrid):
        lvolstruct = self.get_lvolstruct(lvvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])
        save_status = lvolstruct_linear['bind_status']
        self.lvolmap.update_value('lvolid', lvolstruct_linear['lvolid'], 'status', BIND_STATUS['BINDING'])

        self.lvollst.update_value('lvolid', lvvolid, 'hsvrid', hsvrid)
        return save_status

    def bind(self, lvvolid, hsvrid):
        lvolstruct = self.get_lvolstruct(lvvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])

        self.__set_bindstatus(lvolstruct_linear, BIND_STATUS['BOUND'])

        # move the next line to binding().
        #self.lvollst.update_value('lvolid', lvvolid, 'hsvrid', hsvrid)

    def unbinding(self, lvvolid):
        lvolstruct = self.get_lvolstruct(lvvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])
        save_status = lvolstruct_linear['bind_status']
        self.lvolmap.update_value('lvolid', lvolstruct_linear['lvolid'], 'status', BIND_STATUS['UNBINDING'])

        return save_status

    def unbound(self, lvvolid):
        lvolstruct = self.get_lvolstruct(lvvolid)
        lvolstruct_linear = get_lvolstruct_of_lvoltype(lvolstruct, LVOLTYPE['LINEAR'])

        self.__set_bindstatus(lvolstruct_linear, BIND_STATUS['UNBOUND'])

        self.lvollst.update_value('lvolid', lvvolid, 'hsvrid', 0)

    def rebuildfinished_mirror(self, dexts):
        for dext in dexts:
            if dext['dext_status'] == MIRROR_STATUS['VALID']:
                self.lvolmap.update_value('lvolid', dext['dextid'], 'mirror', MIRROR_STATUS['VALID'])

    def replace_disk(self, pdskid):

        result = {}

        sql = """
select 
    lvolmap.* 
from 
    lvolmap, 
    dskmap 
where 
    dskmap.dextid = lvolmap.lvolid and 
    dskmap.pdskid =%s and 
    dskmap.status =%s
        """ % (pdskid, ALLOC_PRIORITY['EVACUATE'])
        self.c.execute(sql)    
        for lvolid, _lvoltype, superlvolid, offset, capacity, status, mirror in self.c.fetchall():

            self.lvolmap.delete_rows('lvolid', lvolid)

            mirror = self.lvolmap.get_row(superlvolid)
            mirrorid = mirror['lvolid']
            lvol = self.lvollst.get_row(mirror['superlvolid'])
            assert(lvol), lineno()
            hsvrid = lvol['hsvrid']
            extents = self.lvolmap.get_rows("superlvolid", superlvolid)
            sqlstr = ""                
            for extent in extents:
                dskmp = self.dskmap.get_row(extent['lvolid'])
                dsk = self.pdsklst.get_row(dskmp['pdskid'])
                sqlstr += "and ssvrid != %s " % dsk['ssvrid']

            # 1) select storage servers have no disk extent on the volume
            # 2) select top 1 storage server in the storage servers at free disk(HIGH) space ratio
            # 3) select top 1 disk(HIGH) in the storage server
            sql = """
select 
    dskspace.pdskid,
    dskspace.ssvrid,
    dskspace.iscsi_path_1,
    dskspace.iscsi_path_2
from 
    (select ssvrid, max(percent) as mp, avg(percent) as avg 
     from dskspace 
     where available >= %s and priority = %s %s 
     group by ssvrid 
     order by avg desc, mp desc limit 1) a, 
    dskspace 
where 
    a.ssvrid=dskspace.ssvrid and 
    dskspace.priority = %s and 
    a.mp=dskspace.percent
            """ % (capacity, ALLOC_PRIORITY['HIGH'], sqlstr, ALLOC_PRIORITY['HIGH'])
            self.c.execute(sql)
            allocated = {}

            result = self.c.fetchall()
            if len(result) < 1:
                # ENOSPC
                raise Exception, "replace_disk: can not choose an alternative disk on some other storage servers"
            pdskid, ssvrid, path, path_2, = result[0]
            # select the largest free extent
            sql = """
select 
    dskmap.pdskid, 
    dskmap.dextid, 
    dskmap.offset, 
    dskmap.capacity, 
    pdsklst.iscsi_path_1, 
    pdsklst.iscsi_path_2 
from 
    dskmap,
    pdsklst 
where 
    dskmap.status = %s and 
    dskmap.pdskid = pdsklst.pdskid and 
    dskmap.pdskid = %s 
order by 
    dskmap.capacity desc
            """ % (EXT_STATUS['FREE'], pdskid)
            self.c.execute(sql)
            result = self.c.fetchall()
            if len(result) < 1:
                # ENOSPC
                raise Exception, "replace_disk: can not allocate a disk extent on some other storage servers"
            pdskid, lvid, offset, l, path, path_2 = result[0]
            add = {'lvolid': lvid, 'capacity': capacity, 'offset': offset, 'ssvrid': ssvrid, 'iscsi_path': (path, path_2)}
            if l > capacity:
                allocated[ssvrid] = True
                # divide the extent before use
                self.dskmap.update_value('dextid', lvid, 'capacity', capacity)
                self.dskmap.update_value('dextid', lvid, 'status', EXT_STATUS['BUSY'])
                if mirror['status'] == BIND_STATUS['ALLOCATED']:
                    # mirror device is not created yet. no need to resync
                    self.lvolmap.put_row(((lvid, LVOLTYPE['DEXT'], mirrorid, 0, capacity, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['VALID'])))
                else:
                    self.lvolmap.put_row(((lvid, LVOLTYPE['DEXT'], mirrorid, 0, capacity, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['INVALID'])))

                freeid = self.genid_lvol.genid()
                if not freeid:
                    # ENOSPC
                    raise Exception, "replace_disk: can not get new dextid"
                self.dskmap.put_row(((pdskid, freeid, offset + capacity, l - capacity, EXT_STATUS['FREE'])))
            elif l == capacity:
                allocated[ssvrid] = True
                # use the extent as it is
                self.dskmap.update_value('dextid', lvid, 'status', EXT_STATUS['BUSY'])
                if mirror['status'] == BIND_STATUS['ALLOCATED']:
                    # mirror device is not created yet. no need to resync
                    self.lvolmap.put_row(((lvid, LVOLTYPE['DEXT'], mirrorid, 0, capacity, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['VALID'])))
                else:
                    self.lvolmap.put_row(((lvid, LVOLTYPE['DEXT'], mirrorid, 0, capacity, BIND_STATUS['ALLOCATED'], MIRROR_STATUS['INVALID'])))

            if len(allocated) == 0:
                # ENOSPC
                raise Exception, "replace_disk: can not allocate a disk extent on some other storage servers"
            
            if status == BIND_STATUS['BOUND']:
                dext = self.dskmap.get_row(lvolid)
                dsk = self.pdsklst.get_row(dext['pdskid'])
                remove = {'lvolid': lvolid, 'ssvrid': dsk['ssvrid'], 'iscsi_path': (dsk['iscsi_path_1'], dsk['iscsi_path_2'])}
                mdinfo = {'id': mirrorid, 'add': add, 'remove': remove, 'superlvolname': lvol['lvolname'], 'superlvolid': lvol['lvolid']}

                if not result.has_key(hsvrid):
                    result[hsvrid] = []
                result[hsvrid].append(mdinfo)

        return result

    def get_dskspace(self):

        result = {}
        ssvr = {}
        dsk = {}
        self.c.execute("select ssvrid, sum(available) from dskspace group by ssvrid")
        for ssvrid, available in self.c.fetchall():
            ssvr[ssvrid] = available
        self.c.execute("select ssvrid, sum(capacity) from pdsklst group by ssvrid")
        for ssvrid, total in self.c.fetchall():
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


class Db_genid(Db_base):
    db_id_max = sys.maxint
    db_newid_holder = 0
    db_oldid_holder = 1
    db_keys = ('id', 'lastid')
    sql_create_table = "(id int primary key, lastid int)"
    sql_insert_into = "(id, lastid) values(%d, %d)"

    def __init__(self):
        pass

    def initialize_tables(self):
        #sql_commit = 'commit'
        sql = "create table %s" % self.db_name + self.sql_create_table
        self.c.execute(sql)

        # initialize an id holder (last issued id)
        sql = "insert into %s(id,lastid) values(%d,%d)" % (self.db_name, self.db_newid_holder, self.db_oldid_holder)
        self.c.execute(sql)

        # initialize an id holder (last recovered id)
        sql = "insert into %s(id,lastid) values(%d,%d)" % (self.db_name, self.db_oldid_holder, self.db_oldid_holder)
        self.c.execute(sql)

        try:
            self.commit()
        except sqlite.DatabaseError:
            self.rollback()

    def lastid(self):
        "report last issued id number."

        # check last issued id
        sql = "select * from %s where id = %d" % (self.db_name, self.db_newid_holder)
        self.c.execute(sql)
        _zero, last_issued_id = self.c.fetchone()

        return last_issued_id

    def __genid_old(self):
        # do not issue new ids any more
        # select oldest recovered id
        sql = "select * from %s where id != %d and lastid = %d" % (self.db_name, self.db_oldid_holder, self.db_oldid_holder)
        self.c.execute(sql)

        oldrecord = self.c.fetchone()
        if oldrecord:
            oldid = oldrecord[0]
        else:
            # no id to re-cycle
            return 0

        # select a next re-cycle candidate or mark 'empty' on id holder.
        sql = "update %s set lastid = %d where lastid = %d" % (self.db_name, self.db_oldid_holder, oldid)
        self.c.execute(sql)

        # delete the re-cycled record.
        sql = "delete from %s where id = %d" % (self.db_name, oldid)
        self.c.execute(sql)

        return(oldid)

    def __genid_new(self, last_issued_id):
        # do not re-cycle recovered ids.
        try:
            # update last issued id holder
            newid = last_issued_id + 1
            sql = "update %s set lastid = %d where id = %d" % (self.db_name, newid, self.db_newid_holder)
            self.c.execute(sql)

            return(newid)    

        except Exception:
            logger.error(traceback.format_exc())
            return(0)

    def genid(self):
        "generate new id number."
        # check last issued id
        sql = "select * from %s where id = %d" % (self.db_name, self.db_newid_holder)
        self.c.execute(sql)
        _zero, last_issued_id = self.c.fetchone()

        if last_issued_id == self.db_id_max:
            # do not issue new ids any more
            return self.__genid_old()
        else:
            # do not re-cycle recovered ids.
            return self.__genid_new(last_issued_id)

    def recover(self, id):
        "recover a free id to re-cycle the id"

        # check last recovered id
        sql = "select * from %s where id = %d" % (self.db_name, self.db_oldid_holder)
        self.c.execute(sql)

        _id_max, last_recovered_id = self.c.fetchone()

        # insert a new record to store a free id
        sql = "insert into %s(id,lastid) values(%d,%d)" % (self.db_name, id, last_recovered_id)
        self.c.execute(sql)

        # update last recovered id holder
        sql = "update %s set lastid = %d where id = %d" % (self.db_name, id, self.db_oldid_holder)
        self.c.execute(sql)

        return(0)

class Db_genid_hsvr(Db_genid):
    def __init__(self):
        self.db_name = 'genid_hsvr'
        self.connect(DB_COMPONENTS)

class Db_genid_ssvr(Db_genid):
    def __init__(self):
        self.db_name = 'genid_ssvr'
        self.connect(DB_COMPONENTS)

class Db_genid_dsk(Db_genid):
    def __init__(self):
        self.db_name = 'genid_dsk'
        self.connect(DB_COMPONENTS)

class Db_genid_lvol(Db_genid):
    def __init__(self):
        self.db_name = 'genid_lvol'
        self.connect(DB_COMPONENTS)

class Db_genid_event(Db_genid):
    def __init__(self):
        self.db_name = 'genid_event'
        self.db_id_max = MAX_EVENTID
        self.connect(DB_EVENTS)

def main():
    # initialize DB

    try:
        Db_ssvrlst().initialize_tables()
        Db_hsvrlst().initialize_tables()
        Db_pdsklst().initialize_tables()
        Db_lvollst().initialize_tables()
        Db_dskmap().initialize_tables()
        Db_lvolmap().initialize_tables()
        Db_transit().initialize_tables()
        Db_resync().initialize_tables()
        Db_shred().initialize_tables()
        
        LogicalVolume().initialize_tables()

        Db_genid_hsvr().initialize_tables()
        Db_genid_ssvr().initialize_tables()
        Db_genid_dsk().initialize_tables()
        Db_genid_lvol().initialize_tables()
        Db_genid_event().initialize_tables()
    except Exception, inst:
        print >> sys.stderr, "DB initialization failed. %s" % (inst)
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
