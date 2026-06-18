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

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Tests for {@link S3EmulatorSupport}.
 */
public class S3EmulatorSupportTest {

    private String previousMode;

    @Before
    public void setUp() {
        previousMode = System.getProperty(S3EmulatorSupport.S3_TEST_MODE_PROP);
    }

    @After
    public void tearDown() {
        if (previousMode == null) {
            System.clearProperty(S3EmulatorSupport.S3_TEST_MODE_PROP);
        } else {
            System.setProperty(S3EmulatorSupport.S3_TEST_MODE_PROP, previousMode);
        }
    }

    @Test
    public void isAvailableReturnsFalseForUnsupportedGcpMode() {
        System.setProperty(S3EmulatorSupport.S3_TEST_MODE_PROP, "GCP");
        Assert.assertFalse(S3EmulatorSupport.isAvailable());
    }

    @Test
    public void isAvailableReturnsTrueInS3ModeWhenDockerAvailable() {
        System.clearProperty(S3EmulatorSupport.S3_TEST_MODE_PROP);
        Assume.assumeTrue("Docker is not available", S3EmulatorSupport.isAvailable());
        Assert.assertTrue(S3EmulatorSupport.isAvailable());
    }

    @Test
    public void getEmulatorPropertiesReturnsEmptyForUnsupportedGcpMode() {
        System.setProperty(S3EmulatorSupport.S3_TEST_MODE_PROP, "GCP");
        Assert.assertTrue(S3EmulatorSupport.getEmulatorProperties().isEmpty());
    }

    @Test
    public void getEmulatorPropertiesReturnsEmptyWhenUnavailable() {
        // When Docker is not available, isAvailable() is false and getEmulatorProperties()
        // must return empty properties rather than throwing.
        Assume.assumeFalse("Docker is available", S3EmulatorSupport.isAvailable());
        Properties props = S3EmulatorSupport.getEmulatorProperties();
        Assert.assertTrue("Expected empty properties when emulator is unavailable", props.isEmpty());
    }

    @Test
    public void getEmulatorPropertiesInS3ModeContainsRequiredKeys() {
        System.clearProperty(S3EmulatorSupport.S3_TEST_MODE_PROP);
        Assume.assumeTrue("Docker is not available", S3EmulatorSupport.isAvailable());

        Properties props = S3EmulatorSupport.getEmulatorProperties();
        Assert.assertNotNull(props.getProperty(S3Constants.ACCESS_KEY));
        Assert.assertNotNull(props.getProperty(S3Constants.SECRET_KEY));
        Assert.assertNotNull(props.getProperty(S3Constants.S3_END_POINT));
        Assert.assertNotNull(props.getProperty(S3Constants.S3_REGION));
        Assert.assertNotNull(props.getProperty(S3Constants.S3_BUCKET));
        Assert.assertEquals("true", props.getProperty(S3Constants.PATH_STYLE_ACCESS));
        Assert.assertNotNull(props.getProperty(S3Constants.S3_MAX_ERR_RETRY));
        Assert.assertNotNull(props.getProperty(S3Constants.S3_CONN_TIMEOUT));
        Assert.assertEquals("http", props.getProperty(S3Constants.S3_CONN_PROTOCOL));
        Assert.assertEquals(S3Constants.S3_ENCRYPTION_NONE, props.getProperty(S3Constants.S3_ENCRYPTION));
        Assert.assertNull("S3 mode must not set MODE", props.get(S3Constants.MODE));
    }
}
