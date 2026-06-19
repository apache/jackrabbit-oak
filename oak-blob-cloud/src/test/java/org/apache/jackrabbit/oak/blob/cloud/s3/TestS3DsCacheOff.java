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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.apache.jackrabbit.oak.spi.blob.data.CachingDataStore;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link CachingDataStore} with S3Backend
 * and local cache Off.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties

 */
public class TestS3DsCacheOff extends TestS3Ds {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3DsCacheOff.class);

    @Override
    @Before
    public void setUp() throws Exception {
        props.setProperty("cacheSize", "0");
        super.setUp();
    }

    // Re-enable: with cache off, deleteRecord is immediately visible via S3 so the
    // assertion that getRecordIfStored returns null after deletion is valid here.
    @Override
    @Test
    public void testDeleteRecord() {
        try {
            doDeleteRecordTest();
        } catch (Exception e) {
            throw new AssertionError("Failed to delete S3 record with cache disabled", e);
        }
    }

    // S3Backend updates duplicate records via CopyObject (copy-to-self). S3Mock does not
    // support that operation, so this test cannot run against the emulator.
    @Override
    @Test
    public void testAddDuplicateRecord() {
        Assume.assumeTrue("S3Mock does not support CopyObject used by S3Backend for duplicate record updates",
                !S3DataStoreUtils.isS3EmulatorConfigured());
        super.testAddDuplicateRecord();
    }
}
