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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

public class FilePackerTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void tinyFile() throws IOException {
        File tiny = temporaryFolder.newFile();
        assertFalse(FilePacker.isPackFile(tiny));
        RandomAccessFile f = new RandomAccessFile(tiny, "rw");
        f.write('P');
        f.close();
        assertFalse(FilePacker.isPackFile(tiny));
        f = new RandomAccessFile(tiny, "rw");
        f.write('P');
        f.write('A');
        f.write('C');
        f.write('K');
        f.close();
        assertTrue(FilePacker.isPackFile(tiny));
    }

    @Test
    public void headerMismatch() throws IOException {
        File pack = temporaryFolder.newFile();
        File dir = temporaryFolder.newFolder();
        RandomAccessFile f = new RandomAccessFile(pack, "rw");
        f.writeUTF("test");
        try {
            assertFalse(FilePacker.isPackFile(dir));
            FilePacker.unpack(pack, dir, false);
            fail();
        } catch (IOException e) {
            e.printStackTrace(System.out);
            assertTrue(e.getMessage(), e.getMessage().startsWith("File header is not 'PACK'"));
        }
        f.close();
        pack.delete();
    }

    @Test
    public void sourceIsDirectory() throws IOException {
        File dir = temporaryFolder.newFolder("source");
        try {
            assertFalse(FilePacker.isPackFile(dir));
            FilePacker.unpack(dir, dir, false);
            fail();
        } catch (IOException e) {
            assertTrue(e.getMessage().startsWith("Source file doesn't exist or is not a file"));
        }
        dir.delete();
    }

    @Test
    public void sourceMissing() throws IOException {
        File dir = temporaryFolder.newFolder("source");
        try {
            assertFalse(FilePacker.isPackFile(new File(dir, "missing")));
            assertFalse(FilePacker.isPackFile(dir));
            FilePacker.unpack(new File(dir, "missing"), dir, false);
            fail();
        } catch (IOException e) {
            assertTrue(e.getMessage().startsWith("Source file doesn't exist or is not a file"));
        }
        dir.delete();
    }

    @Test
    public void targetDirectoryIsFile() throws IOException {
        File target = temporaryFolder.newFile();
        try {
            assertFalse(FilePacker.isPackFile(target));
            FilePacker.unpack(target, target, false);
            fail();
        } catch (IOException e) {
            assertTrue(e.getMessage().startsWith("Target file exists"));
        }
        target.delete();
    }

    @Test
    public void packUnpack() throws IOException {
        packUnpack(false);
        packUnpack(true);
    }

    public void packUnpack(boolean delete) throws IOException {
        File dir = temporaryFolder.newFolder("sourceDir");
        File pack = temporaryFolder.newFile("pack");
        ArrayList<FileEntry> list = new ArrayList<>();
        Random r = new Random(1);
        for (int i = 0; i < 5; i++) {
            FileEntry f = new FileEntry();
            f.file = File.createTempFile("root_", ".txt", dir);
            list.add(f);
            CRC32 crc = new CRC32();
            CheckedOutputStream out = new CheckedOutputStream(
                    new BufferedOutputStream(new FileOutputStream(f.file)),
                    crc);
            // at most ~3 MB
            int s = i * i * 50;
            f.length = i * (s * s + r.nextInt(100000));
            for (int j = 0; j < f.length; j++) {
                out.write(r.nextInt());
            }
            f.contentHash = crc.getValue();
            out.close();
        }
        // for debugging
        // System.out.println(pack.getAbsolutePath());
        // System.out.println(dir.getAbsolutePath());
        FilePacker.pack(dir, TreeSession.getFileNameRegex(), pack, delete);
        assertTrue(FilePacker.isPackFile(pack));

        for (FileEntry f : list) {
            if (delete) {
                assertFalse(f.file.exists());
            } else {
                f.file.delete();
            }
        }
        dir.delete();

        FilePacker.unpack(pack, dir, delete);
        if (delete) {
            assertFalse(pack.exists());
        } else {
            assertTrue(pack.exists());
        }
        for (FileEntry f : list) {
            if (!f.file.exists()) {
                fail();
            }
            CRC32 crc = new CRC32();
            CheckedInputStream in = new CheckedInputStream(
                    new BufferedInputStream(new FileInputStream(f.file)), crc);
            while (in.read() >= 0)
                ;
            in.close();
            assertEquals(f.contentHash, crc.getValue());
        }
        // cleanup
        for (FileEntry f : list) {
            f.file.delete();
        }
        dir.delete();
        pack.delete();
    }

    static class FileEntry {
        File file;
        long length;
        long contentHash;
    }
}
