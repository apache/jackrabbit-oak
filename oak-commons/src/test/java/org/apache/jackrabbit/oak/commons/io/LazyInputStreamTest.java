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

package org.apache.jackrabbit.oak.commons.io;

import static com.google.common.io.Files.asByteSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the LazyInputStream class.
 */
public class LazyInputStreamTest {
    
    private File file;
    
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void test() throws IOException {
        createFile();
        
        // test open / close (without reading)
        LazyInputStream in = new LazyInputStream(asByteSource(file));
        in.close();
        
        // test reading too much and closing too much
        in = new LazyInputStream(asByteSource(file));
        assertEquals(0, in.read());
        assertEquals(-1, in.read());
        assertEquals(-1, in.read());
        assertEquals(-1, in.read());
        in.close();
        in.close();
        in.close();

        // test markSupported, mark, and reset
        in = new LazyInputStream(asByteSource(file));
        assertFalse(in.markSupported());
        in.mark(1);
        assertEquals(0, in.read());
        try {
            in.reset();
            fail();
        } catch (IOException e) {
            // expected
        }
        assertEquals(-1, in.read());
        in.close();
        
        // test read(byte[])
        in = new LazyInputStream(asByteSource(file));
        byte[] test = new byte[2];
        assertEquals(1, in.read(test));
        in.close();        
        
        // test read(byte[],int,int)
        in = new LazyInputStream(asByteSource(file));
        assertEquals(1, in.read(test, 0, 2));
        in.close();        

        // test skip
        in = new LazyInputStream(asByteSource(file));
        assertEquals(2, in.skip(2));
        assertEquals(-1, in.read(test));
        in.close();

        createFile();
        
        // test that the file is closed after reading the last byte
        in = new LazyInputStream(asByteSource(file));
        assertEquals(0, in.read());
        assertEquals(-1, in.read());

        in.close();

        file.delete();
        
    }

    private void createFile() throws IOException {
        file = temporaryFolder.newFile();
        FileOutputStream out = new FileOutputStream(file);
        out.write(new byte[1]);
        out.close();
    }

}
