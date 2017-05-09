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

package org.apache.jackrabbit.oak.segment.standby.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.Test;

public class DefaultStandbyBlobReaderTest {

    private static InputStream newFailingInputStream() {
        return new InputStream() {

            @Override
            public int read() throws IOException {
                throw new IOException("generic error");
            }

        };
    }

    @Test
    public void shouldAlwaysReturnNullWithoutBlobStore() throws Exception {
        DefaultStandbyBlobReader r = new DefaultStandbyBlobReader(null);
        assertNull(r.readBlob("id"));
    }

    @Test
    public void shouldReturnNullIfBlobDoesNotExist() throws Exception {
        BlobStore s = mock(BlobStore.class);
        when(s.getInputStream("id")).thenThrow(new IOException("blob not found"));
        DefaultStandbyBlobReader r = new DefaultStandbyBlobReader(s);
        assertNull(r.readBlob("id"));
    }

    @Test
    public void shouldReturnNullIfBlobIsUnreadable() throws Exception {
        BlobStore s = mock(BlobStore.class);
        when(s.getInputStream("id")).thenReturn(newFailingInputStream());
        DefaultStandbyBlobReader r = new DefaultStandbyBlobReader(s);
        assertNull(r.readBlob("id"));
    }

    @Test
    public void shouldReturnBlobContent() throws Exception {
        BlobStore s = mock(BlobStore.class);
        when(s.getInputStream("id")).thenReturn(new ByteArrayInputStream(new byte[]{1, 2, 3}));
        DefaultStandbyBlobReader r = new DefaultStandbyBlobReader(s);
        assertArrayEquals(new byte[]{1, 2, 3}, r.readBlob("id"));
    }

}
