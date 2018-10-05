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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link org.apache.jackrabbit.core.data.CachingDataStore} with AzureBlobStoreBackend
 * and local cache Off.
 * It requires to pass azure config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties

 */
public class TestAzureDsCacheOff extends TestAzureDS {

    protected static final Logger LOG = LoggerFactory.getLogger(TestAzureDsCacheOff.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        props.setProperty("cacheSize", "0");
    }
}
