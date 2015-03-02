/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.identifier.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

/**
 * Tests for RepositoryManager's mbean registration.
 */
public class RepositoryManagerIT {
	private static final String DB_NAME =  RepositoryManagerIT.class.getSimpleName();
	private static final String MONGO_URI = "mongodb://localhost:27017/" + DB_NAME;
	private static final String BLOB_STORE_PATH = "target/fileDataStore/" + DB_NAME;

	private static MBeanServer mBeanServer;
	private static ObjectName objectName;

	private static DocumentMK documentMK;

	private static RepositoryManagementMBean repositoryManagementMBean;

	private static BlobStore prepareBlobStore(String baseDir, String storeName) {
		FileDataStore fileDS = new FileDataStore();
		fileDS.setMinRecordLength(4092);
		File storeDir = new File(baseDir, storeName);
		fileDS.init(storeDir.getAbsolutePath());
		BlobStore bs = new DataStoreBlobStore(fileDS, true, 100);
		return bs;
	}

	private static void createRepository() throws Throwable {
		DocumentMK.Builder mkBuilder = new DocumentMK.Builder()
				.memoryCacheSize(256 * 1024 * 1024).offHeapCacheSize(0);

		mkBuilder.setBlobStore(prepareBlobStore(BLOB_STORE_PATH, "blob"));

		MongoClientOptions.Builder builder = MongoConnection
				.getDefaultBuilder();
		MongoClientURI mongoURI = new MongoClientURI(MONGO_URI, builder);
		MongoClient mongoClient = new MongoClient(mongoURI);
		DB mongoDB = mongoClient.getDB(DB_NAME);

		mkBuilder.setMaxReplicationLag(TimeUnit.HOURS.toSeconds(6),
				TimeUnit.SECONDS);
		mkBuilder.setMongoDB(mongoDB, 256, 16);

		documentMK = mkBuilder.open();
		ClusterRepositoryInfo.createId(documentMK.getNodeStore());

		Oak oak = new Oak(documentMK.getNodeStore())
			.with(ManagementFactory.getPlatformMBeanServer())
			.withAsyncIndexing();
		Jcr jcr = new Jcr(oak);
		jcr.createRepository();
	}

	@BeforeClass
	public static void init() throws Throwable {
		mBeanServer = ManagementFactory.getPlatformMBeanServer();
		objectName = new ObjectName("org.apache.jackrabbit.oak:name=\"repository manager\",type=\"RepositoryManagement\",id=5");
		createRepository();
		repositoryManagementMBean = JMX.newMBeanProxy(mBeanServer, objectName, RepositoryManagementMBean.class);
	}

	@AfterClass
	public static void destroy() throws Throwable {
		FileUtils.cleanDirectory(new File(BLOB_STORE_PATH));
		documentMK.dispose();
		new MongoConnection(MONGO_URI).getDB().dropDatabase();
	}

	@Test
	public void testDataStoreGC() throws Throwable {
		CompositeData cd = repositoryManagementMBean.startDataStoreGC(false);
		final String expected = "Blob garbage collection running";
        assertEquals(expected, cd.get("message"));
		cd = (CompositeData)mBeanServer.getAttribute(objectName, "DataStoreGCStatus");
		System.out.println(cd);
        assertEquals(expected, cd.get("message"));
	}

	@Test
	public void testRevisionGC() throws Throwable {
		CompositeData cd = repositoryManagementMBean.startRevisionGC();
		final String expected = "Revision garbage collection running";
        assertEquals(expected, cd.get("message"));
		cd = (CompositeData)mBeanServer.getAttribute(objectName, "RevisionGCStatus");
		System.out.println(cd);
        assertEquals(expected, cd.get("message"));
	}

	@Test
	public void testPropertyIndexAsyncReindex() throws Throwable {
		CompositeData cd = repositoryManagementMBean.startPropertyIndexAsyncReindex();
		final String expected = "Property index asynchronous reindex running";
        assertEquals(expected, cd.get("message"));
		cd = (CompositeData)mBeanServer.getAttribute(objectName, "PropertyIndexAsyncReindexStatus");
		System.out.println(cd);
        assertEquals(expected, cd.get("message"));
	}

	@Ignore
	@Test
	public void testBackup() throws Throwable {
		CompositeData cd = repositoryManagementMBean.startBackup();
		final String expected = "?";
        assertEquals(expected, cd.get("message"));
		cd = (CompositeData)mBeanServer.getAttribute(objectName, "BackupStatus");
		System.out.println(cd);
        assertEquals(expected, cd.get("message"));
	}

	@Ignore
	@Test
	public void testRestore() throws Throwable {
		CompositeData cd = repositoryManagementMBean.startRestore();
		final String expected = "?";
        assertEquals(expected, cd.get("message"));
		cd = (CompositeData)mBeanServer.getAttribute(objectName, "RestoreStatus");
		System.out.println(cd);
        assertEquals(expected, cd.get("message"));
	}
}
