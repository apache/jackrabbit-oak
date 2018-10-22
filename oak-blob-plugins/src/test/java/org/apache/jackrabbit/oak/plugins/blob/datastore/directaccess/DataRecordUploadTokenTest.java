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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class DataRecordUploadTokenTest {

    private static final String BLOB_ID = "blob";
    private static final String UPLOAD_ID = "upload";
    private static final byte[] SECRET = "1234567890".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testUploadToken() {
        String encodedToken = new DataRecordUploadToken(BLOB_ID, UPLOAD_ID).getEncodedToken(SECRET);

        // also check token can be parsed and is valid
        DataRecordUploadToken parsedToken = DataRecordUploadToken.fromEncodedToken(encodedToken, SECRET);
        assertEquals(BLOB_ID, parsedToken.getBlobId());
        assertTrue(parsedToken.getUploadId().isPresent());
        assertEquals(UPLOAD_ID, parsedToken.getUploadId().get());
    }

    @Test
    public void testUploadTokenIsAscii() {

        // run a few times to rule out the (low) chance it is ascii just by chance; the seed will change regularly
        for (int i = 0; i < 1000; i++) {
            String encodedToken = new DataRecordUploadToken(BLOB_ID, UPLOAD_ID).getEncodedToken(SECRET);
            assertTrue("upload token is not ascii: " + encodedToken, StringUtils.isAsciiPrintable(encodedToken));

            // also check token can be parsed and is valid
            DataRecordUploadToken parsedToken = DataRecordUploadToken.fromEncodedToken(encodedToken, SECRET);
            assertEquals(BLOB_ID, parsedToken.getBlobId());
            assertTrue(parsedToken.getUploadId().isPresent());
            assertEquals(UPLOAD_ID, parsedToken.getUploadId().get());
        }
    }

    @Test
    public void testUploadTokenSignature() {
        // simple test to check the signature is present and validated
        String spoofedToken = Base64.encodeBase64String((BLOB_ID + "#" + UPLOAD_ID).getBytes(StandardCharsets.UTF_8));

        try {
            DataRecordUploadToken.fromEncodedToken(spoofedToken, SECRET);
        } catch (IllegalArgumentException expected) {
        }
    }
}
