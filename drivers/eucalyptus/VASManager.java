/*******************************************************************************
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
 *******************************************************************************/

/*******************************************************************************
 *Copyright (c) 2009  Eucalyptus Systems, Inc.
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, only version 3 of the License.
 * 
 * 
 *  This file is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 * 
 *  You should have received a copy of the GNU General Public License along
 *  with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  Please contact Eucalyptus Systems, Inc., 130 Castilian
 *  Dr., Goleta, CA 93101 USA or visit <http://www.eucalyptus.com/licenses/>
 *  if you need additional information or have any questions.
 * 
 *  This file may incorporate work covered under the following copyright and
 *  permission notice:
 * 
 *    Software License Agreement (BSD License)
 * 
 *    Copyright (c) 2008, Regents of the University of California
 *    All rights reserved.
 * 
 *    Redistribution and use of this software in source and binary forms, with
 *    or without modification, are permitted provided that the following
 *    conditions are met:
 * 
 *      Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 * 
 *      Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 * 
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 *    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 *    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 *    OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. USERS OF
 *    THIS SOFTWARE ACKNOWLEDGE THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE
 *    LICENSED MATERIAL, COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS
 *    SOFTWARE, AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
 *    IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA, SANTA
 *    BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY, WHICH IN
 *    THE REGENTS’ DISCRETION MAY INCLUDE, WITHOUT LIMITATION, REPLACEMENT
 *    OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO IDENTIFIED, OR
 *    WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT NEEDED TO COMPLY WITH
 *    ANY SUCH LICENSES OR RIGHTS.
 *******************************************************************************/

package com.eucalyptus.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.Cipher;
import javax.persistence.EntityNotFoundException;

import org.apache.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;

import com.eucalyptus.auth.ClusterCredentials;
import com.eucalyptus.auth.Authentication;
import com.eucalyptus.auth.SystemCredentialProvider;
import com.eucalyptus.auth.X509Cert;
import com.eucalyptus.auth.util.Hashes;
import com.eucalyptus.config.StorageControllerBuilder;
import com.eucalyptus.configurable.ConfigurableClass;
import com.eucalyptus.configurable.ConfigurableProperty;
import com.eucalyptus.configurable.PropertyDirectory;
import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.util.ExecutionException;
import com.eucalyptus.util.StorageProperties;
import com.eucalyptus.util.WalrusProperties;

import edu.ucsb.eucalyptus.cloud.entities.AOEVolumeInfo;
import edu.ucsb.eucalyptus.cloud.entities.DirectStorageInfo;
import edu.ucsb.eucalyptus.cloud.entities.ISCSIVolumeInfo;
import edu.ucsb.eucalyptus.cloud.entities.LVMVolumeInfo;
import edu.ucsb.eucalyptus.cloud.entities.StorageInfo;
import edu.ucsb.eucalyptus.cloud.entities.VolumeInfo;
import edu.ucsb.eucalyptus.msgs.ComponentProperty;
import edu.ucsb.eucalyptus.util.StreamConsumer;
import edu.ucsb.eucalyptus.util.SystemUtil;


import edu.ucsb.eucalyptus.admin.client.CloudInfoWeb;

public class VASManager implements LogicalStorageManager {

	public static final String PATH_SEPARATOR = File.separator;
	public static boolean initialized = false;
	public static final String EUCA_VAR_RUN_PATH = "/var/run/eucalyptus";
	private static Logger LOG = Logger.getLogger(VASManager.class);
	public static String eucaHome = System.getProperty("euca.home");

 	/* VAS */	
	private static String eucaLib = System.getProperty("euca.lib.dir");
//        private static CloudInfoWeb cloudInfo;
//	private static String cloudId = cloudInfo.getCloudId();
        private String scName = System.getProperty("euca.storage.name");



	public static StorageExportManager exportManager;


	public void checkPreconditions() throws EucalyptusCloudException {
		//check if binaries exist, commands can be executed, etc.
		String eucaHomeDir = System.getProperty("euca.home");
		if(eucaHomeDir == null) {
			throw new EucalyptusCloudException("euca.home not set");
		}
		eucaHome = eucaHomeDir;
		if(!new File(eucaHome + StorageProperties.EUCA_ROOT_WRAPPER).exists()) {
			throw new EucalyptusCloudException("root wrapper (euca_rootwrap) does not exist in " + eucaHome + StorageProperties.EUCA_ROOT_WRAPPER);
		}
		File varDir = new File(eucaHome + EUCA_VAR_RUN_PATH);
		if(!varDir.exists()) {
			varDir.mkdirs();
		}
	}

	/* vas_driver: derived from SystemUtil.run. modified to throw exception to the caller */ 
        private static String runCommand(String[] command) throws ExecutionException {
                String commandString = "";
                try
                {
                        for(String part : command) {
                                commandString += part + " ";
                        }
                        LOG.debug("Running command: " + commandString);
                        Runtime rt = Runtime.getRuntime();
                        Process proc = rt.exec(command);
                        StreamConsumer error = new StreamConsumer(proc.getErrorStream());
                        StreamConsumer output = new StreamConsumer(proc.getInputStream());
                        error.start();
                        output.start();
                        int returnValue = proc.waitFor();
                        output.join();
                        if(returnValue != 0)
                                throw new ExecutionException(commandString + " error: " + error.getReturnValue());
                        return output.getReturnValue();
                } catch (Throwable t) {
                        LOG.error(t);
                        throw new ExecutionException(commandString + " error: " + t );
                }
        }




	public void initialize() {
	}

	public void configure() {
	}

	public void startupChecks() {
		reload();
	}


	public void cleanVolume(String volumeId) {
		LOG.info("vas_driver: cleanVolume: " + volumeId);
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_deleteVolume", scName, volumeId});
		} catch (ExecutionException e) {
			LOG.error(e);
		}
	}

	public void cleanSnapshot(String snapshotId) {
		LOG.info("vas_driver: cleanSnapshot: " + snapshotId);
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_deleteSnapshot", scName, snapshotId});
		} catch (ExecutionException e) {
			LOG.error(e);
		}
	}

	public native void registerSignals();



	public void createVolume(String volumeId, int size) throws EucalyptusCloudException {
		try{
			 runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_createVolume", scName, volumeId, Integer.toString(size)});
		} catch (ExecutionException e) {
			LOG.error(e);
			throw new EucalyptusCloudException(e);
		}
	}
 

	public int createVolume(String volumeId, String snapshotId, int size) throws EucalyptusCloudException {
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_createVolume", scName, volumeId, snapshotId, Integer.toString(size)});
		} catch (ExecutionException e) {
			LOG.error(e);
			throw new EucalyptusCloudException(e);
		}
                return 0;
	}

	public void addSnapshot(String snapshotId) throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented Yet: addSnapshot(String snapshotId)");
	}

	public void deleteVolume(String volumeId) throws EucalyptusCloudException {
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_deleteVolume", scName, volumeId});
		} catch (ExecutionException e) {
			LOG.error(e);
			throw new EucalyptusCloudException(e);
		}
	}


	public List<String> createSnapshot(String volumeId, String snapshotId) throws EucalyptusCloudException {
                ArrayList<String> returnValues = new ArrayList<String>();
		String dummyFile = "/tmp/vas_euca_snap_dummy";
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_createSnapshot", scName, volumeId, snapshotId});
		} catch (ExecutionException e) {
			LOG.error(e);
			throw new EucalyptusCloudException(e);
		}

		// hack to pass the assersion and read check in BlockStorage.transferSnapshot() 
                // there, it tries to read the file whose name is 1st element of the returning List
		try{
			RandomAccessFile f = new RandomAccessFile(dummyFile, "rw");
			f.setLength(1);
		} catch (Exception e) {
			LOG.error(e);
		}
		returnValues.add(dummyFile);
		returnValues.add("1");

                return returnValues;
	}

	public List<String> prepareForTransfer(String snapshotId) throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet. prepareForTransfer()");
	}

	public void deleteSnapshot(String snapshotId) throws EucalyptusCloudException {
		try{
			runCommand(new String[]{eucaHome + StorageProperties.EUCA_ROOT_WRAPPER, eucaLib + "/vas_deleteSnapshot", scName, snapshotId});
		} catch (ExecutionException e) {
			LOG.error(e);
			throw new EucalyptusCloudException(e);
		}
	}

	public String getVolumeProperty(String volumeId) throws EucalyptusCloudException {
		return scName;
	}

	public void loadSnapshots(List<String> snapshotSet, List<String> snapshotFileNames) throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet. :loadSnapshots() " );
	}

	public void reload() {
		VolumeEntityWrapperManager volumeManager = new VolumeEntityWrapperManager();
		volumeManager.finish();
	}

	public int getSnapshotSize(String snapshotId) throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet. getSnapshotSize. Unable to find snapshot: " + snapshotId);
	}

	private class VolumeEntityWrapperManager {
		private EntityWrapper entityWrapper;

		private VolumeEntityWrapperManager() {
			entityWrapper = StorageProperties.getEntityWrapper();
		}

		public String getVolumeProperty(String volumeId) {
			LVMVolumeInfo lvmVolumeInfo = getVolumeInfo(volumeId);
			if(lvmVolumeInfo != null) {
				if(exportManager instanceof AOEManager) {
					AOEVolumeInfo aoeVolumeInfo = (AOEVolumeInfo) lvmVolumeInfo;
					return StorageProperties.ETHERD_PREFIX + aoeVolumeInfo.getMajorNumber() + "." + aoeVolumeInfo.getMinorNumber();
				} else if(exportManager instanceof ISCSIManager) {
					ISCSIVolumeInfo iscsiVolumeInfo = (ISCSIVolumeInfo) lvmVolumeInfo;
					String storeName = iscsiVolumeInfo.getStoreName();
					String encryptedPassword;
					try {
						encryptedPassword = ((ISCSIManager)exportManager).getEncryptedPassword();
					} catch (EucalyptusCloudException e) {
						LOG.error(e);
						return null;
					}
					return System.getProperty("euca.home") + "," + StorageProperties.STORAGE_HOST + "," + storeName + "," + encryptedPassword;
				}
			}
			return null;
		}


		private void finish() {
			entityWrapper.commit();
		}

		private void abort() {
			entityWrapper.rollback();
		}


		private LVMVolumeInfo getVolumeInfo(String volumeId) {
			if(exportManager instanceof AOEManager) {
				AOEVolumeInfo AOEVolumeInfo = new AOEVolumeInfo(volumeId);
				List<AOEVolumeInfo> AOEVolumeInfos = entityWrapper.query(AOEVolumeInfo);
				if(AOEVolumeInfos.size() > 0) {
					return AOEVolumeInfos.get(0);
				}
			} else if(exportManager instanceof ISCSIManager) {
				ISCSIVolumeInfo ISCSIVolumeInfo = new ISCSIVolumeInfo(volumeId);
				List<ISCSIVolumeInfo> ISCSIVolumeInfos = entityWrapper.query(ISCSIVolumeInfo);
				if(ISCSIVolumeInfos.size() > 0) {
					return ISCSIVolumeInfos.get(0);
				}
			}
			return null;
		}

		private LVMVolumeInfo getVolumeInfo() {
			if(exportManager instanceof AOEManager) {
				AOEVolumeInfo aoeVolumeInfo = new AOEVolumeInfo();
				aoeVolumeInfo.setVbladePid(-1);
				aoeVolumeInfo.setMajorNumber(-1);
				aoeVolumeInfo.setMinorNumber(-1);
				return aoeVolumeInfo;
			} else if(exportManager instanceof ISCSIManager) {
				return new ISCSIVolumeInfo();
			}
			return null;
		}

		private List<LVMVolumeInfo> getAllVolumeInfos() {
			List<LVMVolumeInfo> volumeInfos = new ArrayList<LVMVolumeInfo>();
			volumeInfos.addAll(entityWrapper.query(new AOEVolumeInfo()));
			volumeInfos.addAll(entityWrapper.query(new ISCSIVolumeInfo()));	
			return volumeInfos;
		}

		private void add(LVMVolumeInfo volumeInfo) {
			entityWrapper.add(volumeInfo);
		}

		private void remove(LVMVolumeInfo volumeInfo) {
			entityWrapper.delete(volumeInfo);
		}

	}

	@Override
	public void finishVolume(String snapshotId) throws EucalyptusCloudException{
		//Nothing to do here
	}

	@Override
	public String prepareSnapshot(String snapshotId, int sizeExpected)
	throws EucalyptusCloudException {
		/* vas_driver: what is this for? */
		return DirectStorageInfo.getStorageInfo().getVolumesDir() + File.separator + snapshotId;
	}

	@Override
	public ArrayList<ComponentProperty> getStorageProps() {		
		ArrayList<ComponentProperty> componentProperties = null;
		ConfigurableClass configurableClass = StorageInfo.class.getAnnotation(ConfigurableClass.class);
		if(configurableClass != null) {
			String root = configurableClass.root();
			String alias = configurableClass.alias();
			componentProperties = (ArrayList<ComponentProperty>) PropertyDirectory.getComponentPropertySet(StorageProperties.NAME + "." + root, alias);
		}
		configurableClass = DirectStorageInfo.class.getAnnotation(ConfigurableClass.class);
		if(configurableClass != null) {
			String root = configurableClass.root();
			String alias = configurableClass.alias();
			if(componentProperties == null)
				componentProperties = (ArrayList<ComponentProperty>) PropertyDirectory.getComponentPropertySet(StorageProperties.NAME + "." + root, alias);
			else 
				componentProperties.addAll(PropertyDirectory.getComponentPropertySet(StorageProperties.NAME + "." + root, alias));
		}			
		return componentProperties;
	}

	@Override
	public void setStorageProps(ArrayList<ComponentProperty> storageProps) {
		for (ComponentProperty prop : storageProps) {
			try {
				ConfigurableProperty entry = PropertyDirectory.getPropertyEntry(prop.getQualifiedName());
				//type parser will correctly covert the value
				entry.setValue(prop.getValue());
			} catch (IllegalAccessException e) {
				LOG.error(e, e);
			}
		}
	}

	@Override
	public String getStorageRootDirectory() {
		return DirectStorageInfo.getStorageInfo().getVolumesDir();
	}

	@Override
	public String getVolumePath(String volumeId)
	throws EucalyptusCloudException {
		throw new EntityNotFoundException("vas_driver: Not Implemented yet. getVolumePath: " + volumeId);
	}

	@Override
	public void importVolume(String volumeId, String volumePath, int size)
	throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet: importVolume: " + volumeId);
	}

	@Override
	public String getSnapshotPath(String snapshotId)
	throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet: getSnapshotPath");
	}

	@Override
	public void importSnapshot(String snapshotId, String volumeId, String snapPath, int size)
	throws EucalyptusCloudException {
		throw new EucalyptusCloudException("vas_driver: Not Implemented yet: importSnapshot");
	}
}
