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

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_TAR;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for SegmentNodeStore on S3DataStore GC
 */
@RunWith(Parameterized.class)
public class SegmentS3DataStoreBlobGCIT extends SegmentDataStoreBlobGCIT {
    @Parameterized.Parameter
    public String s3Class;

    protected String bucket;

    private Date startDate;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return S3DataStoreUtils.getFixtures();
    }

    @BeforeClass
    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_TAR));
        assumeTrue(S3DataStoreUtils.isS3Configured());
    }

    @Override
    protected DataStoreBlobStore getBlobStore(File rootFolder) throws Exception {
        Properties props = S3DataStoreUtils.getS3Config();
        bucket = rootFolder.getName();
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        props.setProperty("cacheSize", "0");

        return new DataStoreBlobStore(
            S3DataStoreUtils.getS3DataStore(s3Class, props, rootFolder.getAbsolutePath()));
    }

    @Before
    public void setup() {
        startDate = new Date();
    }

    @After
    public void close() throws Exception {
        if (!Strings.isNullOrEmpty(bucket)) {
            S3DataStoreUtils.deleteBucket(bucket, startDate);
        }
    }
}

