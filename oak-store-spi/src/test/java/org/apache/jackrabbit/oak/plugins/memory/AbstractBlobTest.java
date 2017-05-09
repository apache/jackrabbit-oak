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

package org.apache.jackrabbit.oak.plugins.memory;

import java.io.InputStream;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractBlobTest {
    private Random rnd = new Random();

    @Test
    public void blobComparisonBasedOnContentIdentity() throws Exception {
        byte[] data = bytes(100);
        Blob a = new TestBlob(data, "id1", false);
        Blob b = new TestBlob(data, "id1", false);
        assertTrue(AbstractBlob.equal(a, b));

        Blob a2 = new TestBlob(data, "id1", true);
        Blob b2 = new TestBlob(data, "id2", true);
        assertTrue("Blobs with different id but same content should match", AbstractBlob.equal(a2, b2));
    }

    @Test
    public void blobComparisonBasedOnLength() throws Exception {
        Blob a = new TestBlob(bytes(100), null, false);
        Blob b = new TestBlob(bytes(50), null, false);
        assertFalse("Blob comparison should not fallback on content if lengths not same", AbstractBlob.equal(a, b));
    }

    private byte[] bytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    private static class TestBlob extends ArrayBasedBlob {
        private final String id;
        private final boolean allowAccessToContent;

        public TestBlob(byte[] value, String id, boolean allowAccessToContent) {
            super(value);
            this.id = id;
            this.allowAccessToContent = allowAccessToContent;
        }

        @Override
        public String getContentIdentity() {
            return id;
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            checkState(allowAccessToContent, "Cannot access the stream");
            return super.getNewStream();
        }
    }
}
