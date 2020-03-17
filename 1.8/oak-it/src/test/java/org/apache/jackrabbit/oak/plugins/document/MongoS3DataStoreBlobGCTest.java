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

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.blob.ds.MongoDataStoreBlobGCTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.assumeTrue;

/**
 * Tests DataStoreGC with Mongo and S3
 */
@RunWith(Parameterized.class)
public class MongoS3DataStoreBlobGCTest extends MongoDataStoreBlobGCTest {

    @BeforeClass
    public static void assumptions() {
        assumeTrue(S3DataStoreUtils.isS3Configured());
    }

    @Parameterized.Parameter
    public String s3Class;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return S3DataStoreUtils.getFixtures();
    }

    protected String bucket;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        Properties props = S3DataStoreUtils.getS3Config();
        startDate = new Date();
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        File root = folder.newFolder();
        bucket = root.getName();
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        props.setProperty("cacheSize", "0");

        blobStore = new DataStoreBlobStore(
            S3DataStoreUtils.getS3DataStore(s3Class, props, root.getAbsolutePath()));
        mk = new DocumentMK.Builder().clock(getTestClock()).setMongoDB(mongoConnection.getDB())
            .setBlobStore(blobStore).open();
    }

    @After
    @Override
    public void tearDownConnection() throws Exception {
        S3DataStoreUtils.deleteBucket(bucket, startDate);
        super.tearDownConnection();
    }
}
