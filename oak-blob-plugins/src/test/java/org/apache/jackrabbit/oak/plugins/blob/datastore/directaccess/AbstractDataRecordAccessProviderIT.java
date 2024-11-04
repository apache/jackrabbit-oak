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
package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.Test;

import static org.apache.jackrabbit.guava.common.io.ByteStreams.toByteArray;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractDataRecordAccessProviderIT {

    protected abstract ConfigurableDataRecordAccessProvider getDataStore();
    protected abstract DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract long getProviderMaxPartSize();
    protected abstract HttpsURLConnection getHttpsConnection(long length, URI url) throws IOException;

    protected static long ONE_KB = 1024;
    protected static long ONE_MB = ONE_KB * ONE_KB;
    protected static int expirySeconds = 60*15;

    protected static long TEN_MB = ONE_MB * 10;
    protected static long TWENTY_MB = ONE_MB * 20;
    protected static long ONE_HUNDRED_MB = ONE_MB * 100;

    @Test
    public void testMultiPartDirectUploadIT() throws DataRecordUploadException, DataStoreException, IOException {
        // Disabled by default - this test uses a lot of memory.
        // Execute this test from the command line like this:
        //   mvn test -Dtest=<child-class-name> -Dtest.opts.memory=-Xmx2G
        DataRecordAccessProvider ds = getDataStore();
        for (AbstractDataRecordAccessProviderTest.InitUploadResult res : List.of(
            new AbstractDataRecordAccessProviderTest.InitUploadResult() {
                @Override public long getUploadSize() { return TWENTY_MB; }
                @Override public int getMaxNumURIs() { return 10; }
                @Override public int getExpectedNumURIs() { return 2; }
                @Override public long getExpectedMinPartSize() { return TEN_MB; }
                @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
            },
            new AbstractDataRecordAccessProviderTest.InitUploadResult() {
                @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                @Override public int getMaxNumURIs() { return 10; }
                @Override public int getExpectedNumURIs() { return 10; }
                @Override public long getExpectedMinPartSize() { return TEN_MB; }
                @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
            }
        )) {
            DataRecord uploadedRecord = null;
            try {
                DataRecordUpload uploadContext = ds.initiateDataRecordUpload(res.getUploadSize(), res.getMaxNumURIs());
                assertEquals(res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());

                InputStream in = randomStream(0, res.getUploadSize());

                long uploadSize = res.getUploadSize();
                long uploadPartSize = uploadSize / uploadContext.getUploadURIs().size()
                    + ((uploadSize % uploadContext.getUploadURIs().size()) == 0 ? 0 : 1);

                assertTrue(uploadPartSize <= uploadContext.getMaxPartSize());
                assertTrue(uploadPartSize >= uploadContext.getMinPartSize());

                for (URI uri : uploadContext.getUploadURIs()) {
                    if (0 >= uploadSize) break;

                    long partSize = Math.min(uploadSize, uploadPartSize);
                    uploadSize -= partSize;

                    byte[] buffer = new byte[(int) partSize];
                    in.read(buffer, 0, (int) partSize);

                    doHttpsUpload(new ByteArrayInputStream(buffer), partSize, uri);
                }

                uploadedRecord = ds.completeDataRecordUpload(uploadContext.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = doGetRecord((DataStore) ds, uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);

                in.reset();
                assertTrue(Arrays.equals(toByteArray(in), toByteArray(retrievedRecord.getStream())));
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord((DataStore) ds, uploadedRecord.getIdentifier());
                }
            }
        }
    }

    protected void doHttpsUpload(InputStream in, long contentLength, URI uri) throws IOException {
        HttpsURLConnection conn = getHttpsConnection(contentLength, uri);
        IOUtils.copy(in, conn.getOutputStream());
        int responseCode = conn.getResponseCode();
        assertTrue(conn.getResponseMessage(), responseCode < 400);
    }
}
