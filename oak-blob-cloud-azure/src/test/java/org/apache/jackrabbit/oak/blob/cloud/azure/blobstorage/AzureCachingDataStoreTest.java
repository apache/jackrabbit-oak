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

import static org.junit.Assume.assumeTrue;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.CachingDataStoreTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * Test {@link AbstractSharedCachingDataStore} with AzureDataStore.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties
 */

public class AzureCachingDataStoreTest extends CachingDataStoreTest {
    private static final Logger LOG = LoggerFactory.getLogger(AzureCachingDataStoreTest.class);

    protected Properties props;
    protected String container;
    protected Random randomGen = new Random();

    @BeforeClass
    public static void assumptions() {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());
    }

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @After
    public void tear() {
        try {
            super.tear();
            AzureDataStoreUtils.deleteContainer(container);
        } catch (Exception ignore) {

        }
    }

    @Override
    protected AbstractSharedCachingDataStore createDataStore() throws IOException{
        String datastoreRoot = folder.newFolder().getAbsolutePath();
        props = AzureDataStoreUtils.getAzureConfig();
        container = String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                    + "-test";
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);
        props.setProperty("secret", "123456");
        AzureDataStore ds = new AzureDataStore();
        PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
        ds.setProperties(props);
        return ds;
    }

    @Override
    protected void closeDataStore()  throws DataStoreException {

        try {
            if (!Strings.isNullOrEmpty(container)) {
                AzureDataStoreUtils.deleteContainer(container);
            }
        } catch (Exception e) {
        }
    }

    /**
     * Utility method to stop execution for duration time.
     *
     * @param duration
     *            time in milli seconds
     */
    protected void sleep(long duration) {
        long expected = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < expected) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {

            }
        }
    }
}
