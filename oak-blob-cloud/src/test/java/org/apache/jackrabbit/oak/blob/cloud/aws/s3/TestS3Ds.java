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
package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.util.Date;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils.isS3Configured;
import static org.junit.Assume.assumeTrue;

/**
 * Test {@link org.apache.jackrabbit.core.data.CachingDataStore} with S3Backend and local cache on.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
public class TestS3Ds extends AbstractDataStoreTest {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);

    private Date startTime = null;

    protected Properties props;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(isS3Configured());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        props = getS3Config();
        startTime = new Date();
        String bucket =
            String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                + "-test";
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        super.setUp();
    }

    @Override
    @After
    public void tearDown() {
        try {
            super.tearDown();
            S3DataStoreUtils.cleanup(ds, startTime);
        } catch (Exception ignore) {

        }
    }

    @Override
    protected CachingDataStore createDataStore() throws RepositoryException {
        S3DataStore s3ds = new S3DataStore();
        s3ds.setProperties(props);
        s3ds.setSecret("123456");
        s3ds.init(dataStoreDir);
        sleep(1000);
        return s3ds;
    }
}
