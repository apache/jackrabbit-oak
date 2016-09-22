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

import java.io.IOException;
import java.util.Date;

import org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStoreUtilsTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils.cleanup;
import static org.junit.Assume.assumeTrue;

/**
 * SharedDataStoreUtilsTest for S3.
 */
public class SharedS3DataStoreUtilsTest extends SharedDataStoreUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(SharedS3DataStoreUtilsTest.class);

    @BeforeClass
    public static void assumptions() {
        assumeTrue(S3DataStoreUtils.isS3DataStore());
    }

    @After
    public void close() throws IOException {
        super.close();
        try {
            cleanup(dataStore.getDataStore(), new Date());
        } catch (Exception e) {
            log.error("Error closing data store", e);
        }
    }
}
