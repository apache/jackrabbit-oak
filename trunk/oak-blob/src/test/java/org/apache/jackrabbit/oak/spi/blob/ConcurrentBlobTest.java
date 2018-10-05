/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.blob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.Concurrent;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test concurrent access to the blob store.
 */
public class ConcurrentBlobTest {

    static final byte[] EMPTY = new byte[50];

    MemoryBlobStore store = new MemoryBlobStore();

    @Before
    public void setUp() throws Exception {
        store.setBlockSizeMin(50);
    }

    @Ignore("OAK-1599")
    @Test
    public void test() throws Exception {
        final AtomicInteger id = new AtomicInteger();
        Concurrent.run("blob", new Concurrent.Task() {
            @Override
            public void call() throws Exception {
                int i = id.getAndIncrement();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                IOUtils.writeLong(out, i);
                out.write(EMPTY);
                byte[] data = out.toByteArray();
                String id = store.writeBlob(new ByteArrayInputStream(data));
                Assert.assertEquals(58, store.getBlobLength(id));
                byte[] test = out.toByteArray();
                Assert.assertEquals(8, store.readBlob(id, 0, test, 0, 8));
                Assert.assertTrue(Arrays.equals(data, test));
            }
        });
    }

}
