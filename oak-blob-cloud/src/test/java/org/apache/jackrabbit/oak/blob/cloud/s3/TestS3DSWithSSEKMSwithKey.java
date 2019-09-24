package org.apache.jackrabbit.oak.blob.cloud.s3;


import java.io.ByteArrayInputStream;

import org.apache.jackrabbit.core.data.DataRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;

/**
 * Test S3DataStore operation with SSE_S3 encryption.
 * It requires to pass aws config file via system property  or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties

 */
public class TestS3DSWithSSEKMSwithKey extends TestS3Ds {

        protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSWithSSES3.class);

        protected static long ONE_KB = 1024;
        protected static long ONE_MB = ONE_KB * ONE_KB;
        protected static long ONE_HUNDRED_MB = ONE_MB * 100;
        protected static long ONE_GB = ONE_HUNDRED_MB * 10;

        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            String randomKey = "2bf5110e-98f8-1cc157d96447";
            props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_SSE_KMS);
            props.setProperty(S3Constants.S3_SSE_KMS_KEYID, randomKey);
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

                Assert.assertNotEquals(null, ds);
                rec = ds.getRecord(rec.getIdentifier());
                Assert.assertNotEquals(null, rec);
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
}
