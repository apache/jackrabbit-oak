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
package org.apache.jackrabbit.core.data.db;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import junit.framework.TestCase;
import static org.junit.Assume.assumeTrue;

public class ResettableTempFileInputStreamTest extends TestCase {

    public void testResetStreamAllowsReadAgain() throws Exception {
        final File tmp = createTemporaryFileWithContents(new byte[1]);
        ResettableTempFileInputStream in = null;
        try {
            in = new ResettableTempFileInputStream(tmp);
            assertEquals(0, in.read());
            assertEquals(-1, in.read());
            in.reset();
            assertEquals(0, in.read());
            assertEquals(-1, in.read());
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public void testMarkStreamAllowsReadFromMark() throws Exception {
        final File tmp = createTemporaryFileWithContents(createTestByteArray());
        ResettableTempFileInputStream in = null;
        try {
            in = new ResettableTempFileInputStream(tmp);
            assumeTrue(in.read(new byte[5]) == 5);
            in.mark(Integer.MAX_VALUE);
            assumeTrue(in.read(new byte[5]) == 5);
            in.reset();
            assertEquals(5, in.read());
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private File createTemporaryFileWithContents(byte[] data) throws IOException {
        final File tmp = File.createTempFile("test", ".bin");
        FileUtils.writeByteArrayToFile(tmp, data);
        return tmp;
    }

    private byte[] createTestByteArray() {
        byte[] bytes = new byte[10];
        for (int i = 0; i < 10; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
