

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

__version__ = '$Id: vas_conf.py 334 2010-10-22 03:05:58Z yamamoto2 $'

#
# common for storage manager, head server and storage server nodes.
#

# versions
XMLRPC_VERSION = 5      # please use odd numbers

# directories
VAS_ROOT = '/opt/vas'
STORAGE_MANAGER_BIN = VAS_ROOT + '/bin'
STORAGE_MANAGER_LIB = VAS_ROOT + '/lib'
STORAGE_MANAGER_VAR =  '/var/lib/vas'
STORAGE_MANAGER_CONF = '/etc'
VAS_DEVICE_DIR = '/dev/vas'
VAS_PDSK_DIR = STORAGE_MANAGER_VAR + '/pdsk'
VAS_SNAPSHOT_DIR = STORAGE_MANAGER_VAR + '/snapshot'
DM_DEVICE_DIR = '/dev/mapper'
MD_DEVICE_DIR = '/dev/md'

#
# common parameters
#
SOCKET_DEFAULT_TIMEOUT = 300 #sec
SM_DOWN_RETRY_INTARVAL = 60 #sec
VALID_CHARACTERS_FOR_LVOLNAME = '.-_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
LOG_MAX_FILESIZE = 10485760 # 10MB
LOG_BACKUP_COUNT = 5 #number of log files

#
# for storage manager nodes.
#

# db file
DB_COMPONENTS = STORAGE_MANAGER_VAR + '/db/db_components'

# number of threads
STORAGE_MANAGER_REQUEST_WORKERS = 10

# max number of resync tasks run simultaneously
MAX_RESYNC_TASKS = 16

# max number of shred tasks run simultaneously
MAX_SHRED_TASKS = 72
MAX_SHRED_TASKS_PER_DISK = 1

# minimum and maximum number of dext in a mirror device
MIN_REDUNDANCY = 2
MAX_REDUNDANCY = 9

# maximum capacity(GB) of a logical volume 
MAX_LENGTH = 1024 * 1024 

# supported extent sizes in GB
EXTENTSIZE = [1]

# maximum resource id value. XML-RPC can't handle values larger than 0x7fffffff.
MAX_RESOURCEID = 0x7fffffff

# maximum eventid number
MAX_EVENTID = 1024 * 1024

#
# for head server and storage server nodes.
#
# Storage connectivity driver
# 'srp', 'iscsi'
SCSI_DRIVER = 'srp'

# for srp connectivity
SRP_PATH = '/dev/disk/by-path/srp-%s'

# iSCSI target driver
ISCSI_TARGET = "iet"

# for iSCSI configuration
ISCSI_PATH = '/dev/disk/by-path/ip-%s:3260-iscsi-iqn.%s:%08x-lun-1'
MIN_TID = 1
MAX_TID = 256
LOGIN_TIMEOUT = 60

# for IET iSCSI target configuration
IETADM_PARAMS_LUN = 'Type=blockio'
IETADM_PARAMS_TID = 'MaxRecvDataSegmentLength=65536,MaxXmitDataSegmentLength=65536,MaxBurstLength=65536,FirstBurstLength=8192,InitialR2T=No,Wthreads=128,QueuedCommands=32'

# for TGT iSCSI target configuration
# "--bstype=aio" option should be default in the near future. Although this
# option gives good performance and less cpu load, the kernels included in
# RHEL5.4/CentOS 5.4 or earlier versions don't support the linux aio API.
TGTADM_PARAMS_LUN = "--bstype=rdwr"
#TGTADM_PARAMS_LUN = "--bstype=aio"

# for dmsetup
DMSETUP_RETRY_TIMES = 2
GETMULTIPATHDEVICE_RETRY_TIMES = 10
GETMULTIPATHDEVICE_RETRY_INTERVAL = 10

# for scsi_id
SCSI_ID_RETRY_TIMES = 10

# for blockdev
BLOCKDEV_RETRY_TIMES = 10

# for iscsiadm
ISCSIADM_RETRY_TIMES = 10

# for mdadm
MDADM_CREATE_OPTIONS = "--auto=md --bitmap=internal --metadata=1.1 --run --bitmap-chunk=65536 --delay=5 --level=1"
MDADM_ASSEMBLE_OPTIONS = "--run --auto=md --metadata=1.1"

# for hsvr_agent
GETCONF_ARG_MAX = '/usr/bin/getconf ARG_MAX'

# for shredder
SHREDDER_COUNT = 20
DD_CMD = 'dd'
DD_OPTIONS = 'oflag=direct'

# for hsvr_reporter/ssvr_reporter
REGISTER_DEVICE_LIST = STORAGE_MANAGER_VAR + '/register_device_list'
DEVICE_LIST_FILE = STORAGE_MANAGER_VAR + '/device_list'
HSVRID_FILE = STORAGE_MANAGER_VAR + '/hsvrid'
SSVRID_FILE = STORAGE_MANAGER_VAR + '/ssvrid'
DAEMON_LAUNCHER_CMD = STORAGE_MANAGER_BIN +'/daemon_launcher'
HSVR_AGENT_CMD = STORAGE_MANAGER_BIN +'/hsvr_agent'
SSVR_AGENT_CMD = STORAGE_MANAGER_BIN +'/ssvr_agent'
DISKPATROLLER_CMD = STORAGE_MANAGER_BIN +'/DiskPatroller'
LVOL_ERROR_CMD = STORAGE_MANAGER_BIN +'/lvol_error'
SHUTDOWN_GRACE_COUNT = 3
SHUTDOWN_CMD = 'echo 1' #'shutdown -g0 -h now'
HSVR_AGENT_PID = '/var/run/hsvr_agent.run'
SSVR_AGENT_PID = '/var/run/ssvr_agent.run'
DISKPATROLLER_PID = '/var/run/DiskPatroller.run'
LVOL_ERROR_PID = '/var/run/lvol_error.run'
VAS_SM_RUN = '/var/run/vas_sm.run'
HSVR_REPORTER_PID = '/var/run/hsvr_reporter.run'
SSVR_REPORTER_PID = '/var/run/ssvr_reporter.run'

# for check_servers
SERVER_CHECK_INTERVAL = 30 # seconds

# for lvol_error
LVOL_ERROR_INTERVAL = 10 # seconds

# for DiskPatroller
SCRUB_EXTENT_SIZE = 10240 # MB
SCRUB_STRIPE_SIZE = 16 # MB
SCRUB_FIRST_SLEEP_TIME = 43200 # seconds
SCRUB_SLEEP_TIME = 5 # seconds

# snapshot
SNAPSHOT_CHUNK_SIZE = 32        # in sector

import sys
import socket
import logging, logging.handlers
from ConfigParser import SafeConfigParser

host_storage_manager_list = []
port_storage_manager = 8881
port_hsvr_agent = 8882
port_ssvr_agent = 8883

config = SafeConfigParser()

try:
    config.read("%s/vas.conf" % STORAGE_MANAGER_CONF)

    # indispensable parameters
    if config.has_option("storage_manager", "host_list"):
        csv_strings = config.get("storage_manager", "host_list")
        host_storage_manager_list = csv_strings.split(',')

    if config.has_option("storage_manager", "port"):
        port_storage_manager = config.getint("storage_manager", "port")

    if config.has_option("hsvr_agent", "port"):
        port_hsvr_agent = config.getint("hsvr_agent", "port")

    if config.has_option("ssvr_agent", "port"):
        port_ssvr_agent = config.getint("ssvr_agent", "port")

    if config.has_option("iscsi", "iqn_prefix"):
        iqn_prefix_iscsi = config.get("iscsi", "iqn_prefix")

    if config.has_option("syslog", "host"):
        host_syslog = config.get("syslog", "host")

    # optional(hidden) parameters
    if config.has_option("storage_manager", "max_resync_tasks"):
        MAX_RESYNC_TASKS = config.getint("storage_manager", "max_resync_tasks")

    if config.has_option("storage_manager", "extentsize"):
        csv_strings = config.get("storage_manager", "extentsize")
        EXTENTSIZE = [int(x) for x in csv_strings.split(',')]

    if config.has_option("storage_manager", "shutdown_cmd"):
        SHUTDOWN_CMD = config.get("storage_manager", "shutdown_cmd")

    if config.has_option("storage_manager", "shredder_count"):
        SHREDDER_COUNT = config.getint("storage_manager", "shredder_count")

    if config.has_option("storage_manager", "vas_device_dir"):
        VAS_DEVICE_DIR = config.get("storage_manager", "vas_device_dir")

    if config.has_option("ssvr_agent", "scrub_first_sleep_time"):
        SCRUB_FIRST_SLEEP_TIME = config.getint("ssvr_agent", "scrub_first_sleep_time")

    if config.has_option("ssvr_agent", "scrub_extent_size"):
        SCRUB_EXTENT_SIZE = config.getint("ssvr_agent", "scrub_extent_size")

    if config.has_option("ssvr_agent", "scrub_stripe_size"):
        SCRUB_STRIPE_SIZE = config.getint("ssvr_agent", "scrub_stripe_size")

    if config.has_option("ssvr_agent", "scrub_sleep_time"):
        SCRUB_SLEEP_TIME = config.getint("ssvr_agent", "scrub_sleep_time")

    if config.has_option("ssvr_agent", "dd_cmd"):
        DD_CMD = config.get("ssvr_agent", "dd_cmd")

    if config.has_option("ssvr_agent", "ietadm_params_lun"):
        IETADM_PARAMS_LUN = config.get("ssvr_agent", "ietadm_params_lun")

    if config.has_option("ssvr_agent", "ietadm_params_tid"):
        IETADM_PARAMS_TID = config.get("ssvr_agent", "ietadm_params_tid")

    if config.has_option("ssvr_agent", "target_type"):
        ISCSI_TARGET = config.get("ssvr_agent", "target_type")

    if config.has_option("ssvr_agent", "tgtadm_params_lun"):
        TGTADM_PARAMS_LUN = config.get("ssvr_agent", "tgtadm_params_lun")

    loglevel = logging.INFO
    if config.has_option("syslog", "loglevel"):
        loglevel_str = config.get("syslog", "loglevel")
        if loglevel_str == 'CRITICAL':
            loglevel = logging.CRITICAL
        elif loglevel_str == 'DEBUG':
            loglevel = logging.DEBUG
        elif loglevel_str == 'WARN' or loglevel_str == 'WARNING':
            loglevel = logging.WARN
        elif loglevel_str == 'ERROR':
            loglevel = logging.ERROR
        elif loglevel_str == 'FATAL':
            loglevel = logging.FATAL

except Exception, inst:
    print >> sys.stderr, "loading vas.conf file failed. %s" % (inst)
    # fail through

try:
    logging.basicConfig(level=loglevel, filename='/dev/null')
    prog = sys.argv[0].split("/")[-1]
    if prog in ("hsvr_agent", "ssvr_agent"):
        handler_file = logging.handlers.RotatingFileHandler('/var/log/vas_%s.log' % socket.gethostname(), maxBytes=LOG_MAX_FILESIZE, backupCount=LOG_BACKUP_COUNT)
    else:
        handler_file = logging.handlers.RotatingFileHandler('/var/log/vas_%s.log' % socket.gethostname(), maxBytes=LOG_MAX_FILESIZE, backupCount=0)
    formatter_file = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    handler_file.setFormatter(formatter_file)
    logging.getLogger('').addHandler(handler_file)
    if config.has_option("syslog", "host"):
        handler_syslog = logging.handlers.SysLogHandler((host_syslog, logging.handlers.SYSLOG_UDP_PORT), logging.handlers.SysLogHandler.LOG_USER)
        formatter_syslog = logging.Formatter('%(name)s %(levelname)s %(message)s')
        handler_syslog.setFormatter(formatter_syslog)
        logging.getLogger('').addHandler(handler_syslog)
    logger = logging.getLogger(sys.argv[0].split("/")[-1])
except Exception, inst:
    print >> sys.stderr, "setting up log failed. %s" % (inst)
    # fail through
