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

package org.apache.jackrabbit.oak.segment;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_TAR;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for SegmentNodeStore on S3DataStore GC
 */
@RunWith(Parameterized.class)
public class SegmentS3DataStoreBlobGCIT extends SegmentDataStoreBlobGCIT {
    @Parameterized.Parameter
    public String s3Class;

    protected String bucket;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return S3DataStoreUtils.getFixtures();
    }

    @BeforeClass
    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_TAR));
        assumeTrue(S3DataStoreUtils.isS3Configured());
    }

    protected DataStoreBlobStore getBlobStore(File rootFolder) throws Exception {
        Properties props = S3DataStoreUtils.getS3Config();
        bucket = rootFolder.getName();
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        return new DataStoreBlobStore(
            S3DataStoreUtils.getS3DataStore(s3Class, props, rootFolder.getAbsolutePath()));
    }

    @After
    public void close() throws Exception {
        super.close();
        S3DataStoreUtils.deleteBucket(bucket, startDate);
    }
}

