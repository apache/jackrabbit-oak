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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.junit.Assume.assumeTrue;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.Properties;

import javax.jcr.RepositoryException;

/**
 * Test {@link AzureDataStore} with AzureDataStore and local cache on.
 * It requires to pass azure config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties
 */
public class TestAzureDS extends AbstractDataStoreTest {

  protected static final Logger LOG = LoggerFactory.getLogger(TestAzureDS.class);
  protected Properties props = new Properties();
  protected String container;

  @BeforeClass
  public static void assumptions() {
    assumeTrue(AzureDataStoreUtils.isAzureConfigured());
  }

  @Override
  @Before
  public void setUp() throws Exception {
    props.putAll(AzureDataStoreUtils.getAzureConfig());
    container = String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                + "-test";
    props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);
    props.setProperty("secret", "123456");
    super.setUp();
  }

  @Override
  @After
  public void tearDown() {
    try {
      LogCustomizer customizer = LogCustomizer.forLogger(AzureBlobContainerProvider.class.getName())
              .filter(Level.INFO)
              .create();
      customizer.starting();
      super.tearDown();
      Assert.assertEquals(1, customizer.getLogs().size());
      Assert.assertEquals("Refresh token executor service shutdown completed", customizer.getLogs().get(0));
      AzureDataStoreUtils.deleteContainer(container);
    } catch (Exception ignore) {

    }
  }

  @Override
  protected DataStore createDataStore() throws RepositoryException {
    DataStore azureds = null;
    try {
      azureds = AzureDataStoreUtils.getAzureDataStore(props, dataStoreDir);
    } catch (Exception e) {
      e.printStackTrace();
    }
    sleep(1000);
    return azureds;
  }

  /**---------- Skipped -----------**/
  @Override
  public void testUpdateLastModifiedOnAccess() {
  }

  @Override
  public void testDeleteAllOlderThan() {
  }
}
