package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.junit.Assume.assumeTrue;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import javax.jcr.RepositoryException;

/**
 * Test {@link AzureDataStore} with AzureDataStore and local cache on.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties
 */
public class TestAzureDS extends AbstractDataStoreTest {

  protected static final Logger LOG = LoggerFactory.getLogger(TestAzureDS.class);
  protected Properties props;
  protected String container;

  @BeforeClass
  public static void assumptions() {
    assumeTrue(AzureDataStoreUtils.isAzureConfigured());
  }

  @Override
  @Before
  public void setUp() throws Exception {
    props = AzureDataStoreUtils.getAzureConfig();
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
      super.tearDown();
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
