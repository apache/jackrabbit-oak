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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TarFileTest {

    private File file;

    @Before
    public void setUp() throws IOException {
        file = File.createTempFile("TarFileTest", ".tar");
    }

    @After
    public void tearDown() {
        file.delete();
    }

    @Test
    public void testOpenClose() throws IOException {
        new TarFile(file, 10240, true).close();
        new TarFile(file, 10240, false).close();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        byte[] data = "Hello, World!".getBytes(UTF_8);

        TarFile tar = new TarFile(file, 10240, false);
        try {
            tar.writeEntry(id, data, 0, data.length);
            assertEquals(ByteBuffer.wrap(data), tar.readEntry(msb, lsb));
        } finally {
            tar.close();
        }

        assertEquals(10240, file.length());

        tar = new TarFile(file, 10240, false);
        try {
            assertEquals(ByteBuffer.wrap(data), tar.readEntry(msb, lsb));
        } finally {
            tar.close();
        }
    }

}
