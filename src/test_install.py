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

__version__ = '$Id: test_install.py 327 2010-10-22 02:50:09Z yamamoto2 $'

import py_compile
import sys
import os
import getopt
import shutil

root = "/"

directories = (
"/etc/init.d",
"/usr/bin",
"/usr/sbin",
"/opt/vas/bin",
"/opt/vas/lib",
"/var/lib/vas",
"/var/lib/vas/db",
)

cmd_and_lib_targets = (
"/usr/bin/lvol_create", 
"/usr/bin/lvol_delete", 
"/usr/bin/lvol_attach", 
"/usr/bin/lvol_detach", 
"/usr/bin/lvol_list", 
"/usr/bin/lvol_show", 
"/usr/bin/hsvr_list", 
"/usr/bin/snap_create", 
"/usr/bin/ssvr_list", 
"/usr/bin/pdsk_list", 
"/usr/sbin/hsvr_delete", 
"/usr/sbin/ssvr_delete", 
"/usr/sbin/pdsk_delete", 
"/opt/vas/bin/storage_manager", 
"/opt/vas/bin/hsvr_agent", 
"/opt/vas/bin/ssvr_agent", 
"/opt/vas/bin/daemon_launcher", 
"/opt/vas/bin/hsvr_reporter", 
"/opt/vas/bin/ssvr_reporter", 
"/opt/vas/bin/shutdownAll", 
"/opt/vas/bin/vas_db", 
"/opt/vas/bin/check_servers", 
"/opt/vas/bin/DiskPatroller", 
"/opt/vas/bin/lvol_error", 
)

lib_targets = (
"/opt/vas/lib/vas_subr.pyc", 
"/opt/vas/lib/vas_conf.pyc", 
"/opt/vas/lib/vas_const.pyc", 
"/opt/vas/lib/vas_iscsi.pyc",
"/opt/vas/lib/dag.pyc", 
"/opt/vas/lib/event.pyc", 
"/opt/vas/lib/hashedlock.pyc", 
"/opt/vas/lib/hsvr_dag.pyc", 
"/opt/vas/lib/lv_dbnode.pyc", 
"/opt/vas/lib/lvnode.pyc", 
"/opt/vas/lib/lvnode_0.pyc", 
"/opt/vas/lib/lvnode_1.pyc", 
"/opt/vas/lib/lvnode_2.pyc", 
"/opt/vas/lib/lvnode_3.pyc", 
"/opt/vas/lib/lvnode_6.pyc", 
"/opt/vas/lib/lvnode_7.pyc", 
"/opt/vas/lib/mynode.pyc", 
"/opt/vas/lib/refcountedhash.pyc", 
"/opt/vas/lib/symlinknode.pyc", 
"/opt/vas/lib/worker.pyc", 
)

simple_targets = (
"/etc/init.d/vas_sm",
"/etc/init.d/vas_hsvr",
"/etc/init.d/vas_ssvr",
)

def install():
    for dir in directories:
        try:
            os.stat(root + dir)
        except:
            os.makedirs(root + dir)

    for target in cmd_and_lib_targets:
        basename = os.path.basename(target)
        file = "%s.py" % basename
        cfile = "/opt/vas/lib/%s.pyc" % basename
        py_compile.compile(file, root + cfile)
        os.chmod(root + cfile, 0644)

        f = open(root + target, "w")
        f.writelines("#!/usr/bin/python\n")
        f.writelines("import sys\n")
        f.writelines("sys.path.insert(0, '"'/opt/vas/lib'"')\n")
        f.writelines("import %s\n" % basename)
        f.writelines("%s.main()\n" % basename)
        f.close()
        os.chmod(root + target, 0755)

    for target in lib_targets:
        file = os.path.basename(target)[:-1]
        py_compile.compile(file, root + target)
        os.chmod(root + target, 0644)

    for target in simple_targets:
        basename = os.path.basename(target)
        shutil.copyfile(basename, root + target)
        os.chmod(root + target, 0755)

def uninstall():
    for target in cmd_and_lib_targets:
        basename = os.path.basename(target)
        cfile = "/opt/vas/lib/%s.pyc" % basename
        os.remove(root + cfile)
        os.remove(root + target)

    for target in lib_targets:
        os.remove(root + target)

    for target in simple_targets:
        os.remove(root + target)

def usage():
    print 'usage: %s [-r|--root=rootdir] [-u|--uninstall] [--help] ' \
        % sys.argv[0]

def main():
    global root
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:u", \
            ["uninstall","help","root="])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for o, a in opts:
        if o == "--help":
            usage()
            sys.exit(2)
        elif o in ("-u", "--uninstall"):
            sys.exit(uninstall())
        elif o in ("--root"):
            root = a
    sys.exit(install())

if __name__ == "__main__":
    main()
