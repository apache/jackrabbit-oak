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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;

import org.apache.jackrabbit.oak.spi.blob.data.CachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.data.LocalCache;
import org.junit.Before;

/**
 * Test {@link CachingDataStore} with AzureBlobStoreBackendV12 and with very small size
 * {@link LocalCache}. Uses Azurite (local Azure emulator) via Docker for testing.
 */
public class TestAzureDSWithSmallCacheV12 extends TestAzureDSV12 {

  @Override
    @Before
    public void setUp() throws Exception {
        props.setProperty("cacheSize", String.valueOf(dataLength * 10));
        super.setUp();
    }
}
