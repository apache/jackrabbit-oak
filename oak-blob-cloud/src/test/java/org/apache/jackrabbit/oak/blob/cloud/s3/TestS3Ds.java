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

import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.matchers.Equals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assume.assumeTrue;

/**
 * Test {@link S3DataStore} with S3Backend and local cache on.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
@RunWith(Parameterized.class)
public class TestS3Ds extends AbstractDataStoreTest {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);

    private Date startTime = null;

    protected Properties props;

    protected String bucket;

    @Parameterized.Parameter
    public String s3Class;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return getFixtures();
    }

    @BeforeClass
    public static void assumptions() {
        assumeTrue(isS3Configured());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        props = getS3Config();
        startTime = new Date();
        bucket =
            String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                + "-test";
        props.setProperty(S3Constants.S3_BUCKET, bucket);
        props.setProperty("secret", "123456");
        super.setUp();
    }

    @Override
    @After
    public void tearDown() {
        try {
            super.tearDown();
            S3DataStoreUtils.deleteBucket(bucket, startTime);
        } catch (Exception ignore) {

        }
    }

    protected DataStore createDataStore() throws RepositoryException {
        DataStore s3ds = null;
        try {
            s3ds = getS3DataStore(s3Class, props, dataStoreDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep(1000);
        return s3ds;
    }

    /**----------Not supported-----------**/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }
}
