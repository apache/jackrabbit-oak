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
package org.apache.jackrabbit.oak.plugins.document.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#getLength(String)}
 */
public class DocumentMKGetLengthGridFSTest extends AbstractMongoConnectionTest {

    @Test
    public void nonExistent() throws Exception {
        try {
            mk.getLength("nonExistentBlob");
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void small() throws Exception {
        getLength(1024);
    }

    @Test
    public void medium() throws Exception {
        getLength(1024 * 1024);
    }

    @Test
    public void large() throws Exception {
        getLength(20 * 1024 * 1024);
    }

    private void getLength(int blobLength) throws Exception {
        String blobId = createAndWriteBlob(blobLength);
        long length = mk.getLength(blobId);
        assertEquals(blobLength, length);
    }

    private String createAndWriteBlob(int blobLength) {
        byte[] blob = createBlob(blobLength);
        return mk.write(new ByteArrayInputStream(blob));
    }

    private static byte[] createBlob(int blobLength) {
        byte[] blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) i;
        }
        return blob;
    }
}