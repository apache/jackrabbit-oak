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

package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.io.ByteArrayInputStream;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.blob.cloud.S3DataStoreUtils;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.junit.Assert;
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
public class TestS3DSWithSSES3 extends TestS3Ds {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSWithSSES3.class);

    @Override
    protected CachingDataStore createDataStore() throws RepositoryException {
        props.setProperty(S3Constants.S3_ENCRYPTION,
            S3Constants.S3_ENCRYPTION_SSE_S3);
        S3DataStore s3ds = new S3DataStore();
        s3ds.setProperties(props);
        s3ds.setSecret("123456");
        s3ds.init(dataStoreDir);
        sleep(1000);
        return s3ds;
    }

    /**
     * Test data migration enabling SSE_S3 encryption.
     */
    @Test
    public void testDataMigration() {
        try {
            String bucket = props.getProperty(S3Constants.S3_BUCKET);
            S3DataStore s3ds = new S3DataStore();
            s3ds.setProperties(props);
            s3ds.setCacheSize(0);
            s3ds.init(dataStoreDir);
            byte[] data = new byte[dataLength];
            randomGen.nextBytes(data);
            DataRecord rec = s3ds.addRecord(new ByteArrayInputStream(data));
            Assert.assertEquals(data.length, rec.getLength());
            assertRecord(data, rec);
            s3ds.close();

            // turn encryption now.
            props.setProperty(S3Constants.S3_BUCKET, bucket);
            props.setProperty(S3Constants.S3_ENCRYPTION,
                S3Constants.S3_ENCRYPTION_SSE_S3);
            props.setProperty(S3Constants.S3_RENAME_KEYS, "true");
            s3ds = new S3DataStore();
            s3ds.setProperties(props);
            s3ds.setCacheSize(0);
            s3ds.init(dataStoreDir);

            rec = s3ds.getRecord(rec.getIdentifier());
            Assert.assertEquals(data.length, rec.getLength());
            assertRecord(data, rec);

            randomGen.nextBytes(data);
            rec = s3ds.addRecord(new ByteArrayInputStream(data));
            s3ds.close();

        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

}
