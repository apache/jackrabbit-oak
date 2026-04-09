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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;

import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.Properties;

/**
 * Test {@link AzureDataStore} with AzureBlobStoreBackendV12 and local cache on.
 * Uses Azurite (local Azure emulator) via Docker for testing.
 */
public class TestAzureDSV12 extends AbstractDataStoreTest {

  @ClassRule
  public static AzuriteDockerRule azurite = new AzuriteDockerRule();

  protected Properties props = new Properties();
  protected String container;

  @Override
  @Before
  public void setUp() throws Exception {
    props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, azurite.getConnectionString());
    props.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
    props.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, azurite.getBlobEndpoint());
    props.setProperty(AzureConstants.AZURE_V12_ENABLED_PROPERTY, "true");
    container = randomGen.nextInt(9999) + "-" + randomGen.nextInt(9999)
                + "-test";
    props.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, container);
    props.setProperty(AzureConstantsV12.AZURE_CREATE_CONTAINER, "true");
    props.setProperty("secret", "123456");
    super.setUp();
  }

  @Override
  @After
  public void tearDown() {
    try {
      super.tearDown();
      // Clean up test container in Azurite
      azurite.getContainer(container, azurite.getConnectionString()).deleteIfExists();
    } catch (Exception ignore) {

    }
  }

  @Override
  protected DataStore createDataStore() {
    DataStore azureds = null;
    try {
      AzureDataStore ds = new AzureDataStore();
      org.apache.jackrabbit.oak.commons.PropertiesUtil.populate(
              ds, org.apache.jackrabbit.oak.commons.collections.MapUtils.fromProperties(props), false);
      ds.setProperties(props);
      ds.init(dataStoreDir);
      azureds = ds;
    } catch (Exception e) {
      e.printStackTrace();
    }
    sleep(1000);
    return azureds;
  }

  /**---------- Skipped (not supported by CachingDataStore on Azurite) -----------**/
  @Override
  public void testUpdateLastModifiedOnAccess() {
  }

  @Override
  public void testDeleteAllOlderThan() {
  }

  @Override
  public void testDeleteRecord() {
  }
}
