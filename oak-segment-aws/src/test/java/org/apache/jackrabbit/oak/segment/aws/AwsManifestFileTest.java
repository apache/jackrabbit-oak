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
package org.apache.jackrabbit.oak.segment.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3;

import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AwsManifestFileTest {

    @ClassRule
    public static final S3MockRule s3Mock = new S3MockRule();

    private S3Directory directory;

    @Before
    public void setup() throws IOException {
        AmazonS3 s3 = s3Mock.createClient();
        long time = new Date().getTime();
        directory = new S3Directory(s3, "bucket-" + time, "oak");
        directory.ensureBucket();
    }

    @Test
    public void testManifest() throws URISyntaxException, IOException {
        ManifestFile manifestFile = new AwsManifestFile(directory, "manifest");
        assertFalse(manifestFile.exists());

        Properties props = new Properties();
        props.setProperty("xyz", "abc");
        props.setProperty("version", "123");
        manifestFile.save(props);

        assertTrue(manifestFile.exists());

        Properties loaded = manifestFile.load();
        assertEquals(props, loaded);
    }
}
