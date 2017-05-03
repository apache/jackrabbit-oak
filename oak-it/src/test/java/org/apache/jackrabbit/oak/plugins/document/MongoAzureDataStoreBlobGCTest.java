/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assume.assumeTrue;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.blob.ds.MongoDataStoreBlobGCTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.Date;
import java.util.Properties;

/**
 * Tests DataStoreGC with Mongo and Azure
 */
public class MongoAzureDataStoreBlobGCTest extends MongoDataStoreBlobGCTest {

    @BeforeClass
    public static void assumptions() {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());
    }

    protected String bucket;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        Properties props = AzureDataStoreUtils.getAzureConfig();
        startDate = new Date();
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        File root = folder.newFolder();
        bucket = root.getName();
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, bucket);
        props.setProperty("cacheSize", "0");
        blobStore = new DataStoreBlobStore(
            AzureDataStoreUtils.getAzureDataStore(props, root.getAbsolutePath()));
        mk = new DocumentMK.Builder().clock(getTestClock()).setMongoDB(mongoConnection.getDB())
            .setBlobStore(blobStore).open();
    }

    @After
    @Override
    public void tearDownConnection() throws Exception {
        AzureDataStoreUtils.deleteContainer(bucket);
        super.tearDownConnection();
    }
}
