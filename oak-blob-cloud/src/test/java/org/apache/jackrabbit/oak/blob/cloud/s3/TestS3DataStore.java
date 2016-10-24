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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

/**
 * Simple tests for S3DataStore.
 */
@RunWith(Parameterized.class)
public class TestS3DataStore {
    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Properties props;

    @Parameterized.Parameter
    public String s3Class;

    private File dataStoreDir;

    private DataStore ds;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return getFixtures();
    }

    @Before
    public void setUp() throws Exception {
        dataStoreDir = folder.newFolder();
        props = new Properties();
    }

    @Test
    public void testAccessParamLeakOnError() throws Exception {
        expectedEx.expect(RepositoryException.class);
        expectedEx.expectMessage("Could not initialize S3 from {s3Region=us-standard}");

        props.put(S3Constants.ACCESS_KEY, "abcd");
        props.put(S3Constants.SECRET_KEY, "123456");
        props.put(S3Constants.S3_REGION, "us-standard");
        ds = getS3DataStore(s3Class, props, dataStoreDir.getAbsolutePath());
    }

    @Test
    public void testNoSecretDefined() throws Exception {
        assumeTrue(isS3Configured());
        Random randomGen = new Random();

        props = S3DataStoreUtils.getS3Config();
        ds = getS3DataStore(s3Class, props, dataStoreDir.getAbsolutePath());
        byte[] data = new byte[4096];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        Assert.assertEquals(data.length, rec.getLength());
        assertNull(rec.getReference());
    }
}
