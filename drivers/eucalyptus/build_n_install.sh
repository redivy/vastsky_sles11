#!/bin/sh

# $Id: build_n_install.sh 338 2010-10-22 03:22:20Z sugihara $

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

set -x
set -e

# set the top directory of previously built src
SRC_TOPDIR=/path/to/eucalyptus-2.0.0/
SRC_TOPDIR=~/eucalyptus/eucalyptus-2.0.0-dev2/

# build and deploy for clc
cp VASManager.java $SRC_TOPDIR/clc/modules/storage-controller/src/main/java/com/eucalyptus/storage/
cd  $SRC_TOPDIR/clc
export JAVA_HOME=/usr/java/jdk1.6.0_21/ # this is needed for fedora. according to eucalyptus online docs
make
make install
cd - 

# now it's time for nc
cp vas_nc.against-v2.0.patch $SRC_TOPDIR/node
cd $SRC_TOPDIR/node 
patch < vas_nc.against-v2.0.patch
make
make install
cd -

# finally, copy bunch of callee scripts under the library directory,
# assuming eucalyptus is installed under /opt/eucalyptus.

install -o root -m 755 vas_euca_driver.py /opt/eucalyptus/usr/share/eucalyptus/


install -o root -m 755 vas_sc_driver.py /opt/eucalyptus/usr/share/eucalyptus/vas_createVolume
install -o root -m 755 vas_sc_driver.py  /opt/eucalyptus/usr/share/eucalyptus/vas_deleteVolume
install -o root -m 755 vas_sc_driver.py  /opt/eucalyptus/usr/share/eucalyptus/vas_createSnapshot
install -o root -m 755 vas_sc_driver.py  /opt/eucalyptus/usr/share/eucalyptus/vas_deleteSnapshot


install -o root -m 755 vas_nc_driver.py /opt/eucalyptus/usr/share/eucalyptus/vas_attachVolume
install -o root -m 755 vas_nc_driver.py /opt/eucalyptus/usr/share/eucalyptus/vas_detachVolume

echo Done.
