package org.apache.jackrabbit.oak.blob.cloud.s3;


import java.io.ByteArrayInputStream;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.junit.Assert.fail;

/**
 * Test S3DataStore operation with SSE_S3 encryption.
 * It requires to pass aws config file via system property  or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties

 */
public class TestS3DSWithSSEKMS extends TestS3Ds {

        protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSWithSSES3.class);

        protected static long ONE_KB = 1024;
        protected static long ONE_MB = ONE_KB * ONE_KB;
        protected static long TEN_MB = ONE_MB * 10;
        protected static long ONE_HUNDRED_MB = ONE_MB * 100;
        protected static long ONE_GB = ONE_HUNDRED_MB * 10;
        
        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            props = getS3Config();
            props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_SSE_KMS);
            props.setProperty("cacheSize", "0");
        }

        /**
         * Test data migration enabling SSE_KMS encryption.
         */
        @Test
        public void testDataMigration() {
            try {
                //manually close the setup ds and remove encryption
                ds.close();
                props.remove(S3Constants.S3_ENCRYPTION);
                ds = createDataStore();

                byte[] data = new byte[dataLength];
                randomGen.nextBytes(data);
                DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
                Assert.assertEquals(data.length, rec.getLength());
                assertRecord(data, rec);
                ds.close();

                // turn encryption now anc recreate datastore instance
                props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_SSE_KMS);
                props.setProperty(S3Constants.S3_RENAME_KEYS, "true");
                ds = createDataStore();

                rec = ds.getRecord(rec.getIdentifier());
                Assert.assertEquals(data.length, rec.getLength());
                assertRecord(data, rec);

                randomGen.nextBytes(data);
                ds.addRecord(new ByteArrayInputStream(data));
                ds.close();
            } catch (Exception e) {
                LOG.error("error:", e);
                fail(e.getMessage());
            }
        }

        @Test
        public void testInitiateDirectUploadUnlimitedURIs() throws DataRecordUploadException {
            ConfigurableDataRecordAccessProvider ds = null;
            try {
                ds = (ConfigurableDataRecordAccessProvider) createDataStore();
            } catch (RepositoryException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            long uploadSize = ONE_GB * 50;
            int expectedNumURIs = 5000;
            DataRecordUpload upload = ds.initiateDataRecordUpload(uploadSize, -1);
            Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());

            uploadSize = ONE_GB * 100;
            expectedNumURIs = 10000;
            upload = ds.initiateDataRecordUpload(uploadSize, -1);
            Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());

            uploadSize = ONE_GB * 200;
            // expectedNumURIs still 10000, AWS limit
            upload = ds.initiateDataRecordUpload(uploadSize, -1);
            Assert.assertEquals(expectedNumURIs, upload.getUploadURIs().size());
        }
    }
