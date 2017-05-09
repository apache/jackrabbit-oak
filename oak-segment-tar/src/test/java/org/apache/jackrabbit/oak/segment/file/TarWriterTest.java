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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void createNextGenerationTest() throws IOException {
        int counter = 2222;
        TarWriter t0 = new TarWriter(folder.newFolder(), FileStoreMonitor.DEFAULT, counter, new IOMonitorAdapter());

        // not dirty, will not create a new writer
        TarWriter t1 = t0.createNextGeneration();
        assertEquals(t0, t1);
        assertTrue(t1.getFile().getName().contains("" + counter));

        // dirty, will create a new writer
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(UTF_8);
        t1.writeEntry(msb, lsb, data, 0, data.length, 0);

        TarWriter t2 = t1.createNextGeneration();
        assertNotEquals(t1, t2);
        assertTrue(t1.isClosed());
        assertTrue(t2.getFile().getName().contains("" + (counter + 1)));
    }

}
