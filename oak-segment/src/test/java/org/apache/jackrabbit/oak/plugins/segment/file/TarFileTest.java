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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarFileTest {

    private File file;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws IOException {
        file = folder.newFile();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(UTF_8);

        TarWriter writer = new TarWriter(file);
        try {
            writer.writeEntry(msb, lsb, data, 0, data.length);
            assertEquals(ByteBuffer.wrap(data), writer.readEntry(msb, lsb));
        } finally {
            writer.close();
        }

        assertEquals(4096, file.length());

        TarReader reader = TarReader.open(file, false);
        try {
            assertEquals(ByteBuffer.wrap(data), reader.readEntry(msb, lsb));
        } finally {
            reader.close();
        }

        reader = TarReader.open(file, false);
        try {
            assertEquals(ByteBuffer.wrap(data), reader.readEntry(msb, lsb));
        } finally {
            reader.close();
        }
    }

}
