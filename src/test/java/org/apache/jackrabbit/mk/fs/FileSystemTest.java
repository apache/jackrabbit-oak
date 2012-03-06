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
package org.apache.jackrabbit.mk.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.jackrabbit.mk.util.IOUtilsTest;

/**
 * Tests various file system.
 */
public class FileSystemTest extends TestCase {

    private String getBaseDir() {
        return "target/temp";
    }

    public void test() throws Exception {
        testFileSystem(getBaseDir() + "/fs");
        testFileSystem("cache:" + getBaseDir() + "/fs");
    }

    public void testAbsoluteRelative() {
        assertTrue(FileUtils.isAbsolute("/test/abc"));
        assertFalse(FileUtils.isAbsolute("test/abc"));
        assertTrue(FileUtils.isAbsolute("~/test/abc"));
    }

    public void testClasspath() throws IOException {
        String resource = getClass().getName().replace('.', '/') + ".class";
        InputStream in;
        in = getClass().getResourceAsStream("/" + resource);
        assertTrue(in != null);
        in.close();
        in = getClass().getClassLoader().getResourceAsStream(resource);
        assertTrue(in != null);
        in.close();
        in = FileUtils.newInputStream("classpath:" + resource);
        assertTrue(in != null);
        in.close();
        in = FileUtils.newInputStream("classpath:/" + resource);
        assertTrue(in != null);
        in.close();
    }

    public void testSimpleExpandTruncateSize() throws Exception {
        String f = getBaseDir() + "/fs/test.data";
        FileUtils.createDirectories(getBaseDir() + "/fs");
        FileChannel c = FileUtils.open(f, "rw");
        c.position(4000);
        c.write(ByteBuffer.wrap(new byte[1]));
        FileLock lock = c.tryLock();
        c.truncate(0);
        if (lock != null) {
            lock.release();
        }
        c.close();
    }

    public void testUserHome() throws IOException {
        String userDir = System.getProperty("user.home").replace('\\', '/');
        assertTrue(FileUtils.toRealPath("~/test").startsWith(userDir));
        assertTrue(FileUtils.toRealPath("file:~/test").startsWith(userDir));
    }

    private void testFileSystem(String fsBase) throws Exception {
        testAppend(fsBase);
        testDirectories(fsBase);
        testMoveTo(fsBase);
        testParentEventuallyReturnsNull(fsBase);
        testRandomAccess(fsBase);
        testResolve(fsBase);
        testSetReadOnly(fsBase);
        testSimple(fsBase);
        testTempFile(fsBase);
        testUnsupportedFeatures(fsBase);
    }

    public void testAppend(String fsBase) throws IOException {
        String fileName = fsBase + "/testFile.txt";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        FileUtils.createDirectories(FileUtils.getParent(fileName));
        FileUtils.createFile(fileName);
        // Profiler prof = new Profiler();
        // prof.interval = 1;
        // prof.startCollecting();
        FileChannel c = FileUtils.open(fileName, "rw");
        c.position(0);
        // long t = System.currentTimeMillis();
        byte[] array = new byte[100];
        ByteBuffer buff = ByteBuffer.wrap(array);
        for (int i = 0; i < 100000; i++) {
            array[0] = (byte) i;
            c.write(buff);
            buff.rewind();
        }
        c.close();
        // System.out.println(fsBase + ": " + (System.currentTimeMillis() - t));
        // System.out.println(prof.getTop(10));
        FileUtils.delete(fileName);
    }

    private void testDirectories(String fsBase) throws IOException {
        final String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            try {
                FileUtils.createDirectory(fileName);
                fail();
            } catch (IOException e) {
                // expected
            }
            try {
                FileUtils.createDirectories(fileName + "/test");
                fail();
            } catch (IOException e) {
                // expected
            }
            FileUtils.delete(fileName);
        }
    }

    private void testMoveTo(String fsBase) throws IOException {
        final String fileName = fsBase + "/testFile";
        final String fileName2 = fsBase + "/testFile2";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            FileUtils.moveTo(fileName, fileName2);
            FileUtils.createFile(fileName);
            try {
                FileUtils.moveTo(fileName2, fileName);
                fail();
            } catch (IOException e) {
                // expected
            }
            FileUtils.delete(fileName);
            FileUtils.delete(fileName2);
            try {
                FileUtils.moveTo(fileName, fileName2);
                fail();
            } catch (IOException e) {
                // expected
            }
        }
    }

    private void testParentEventuallyReturnsNull(String fsBase) {
        FilePath p = FilePath.get(fsBase + "/testFile");
        assertTrue(p.getScheme().length() > 0);
        for (int i = 0; i < 100; i++) {
            if (p == null) {
                return;
            }
            p = p.getParent();
        }
        fail("Parent is not null: " + p);
        String path = fsBase + "/testFile";
        for (int i = 0; i < 100; i++) {
            if (path == null) {
                return;
            }
            path = FileUtils.getParent(path);
        }
        fail("Parent is not null: " + path);
    }

    private void testResolve(String fsBase) {
        String fileName = fsBase + "/testFile";
        assertEquals(fileName, FilePath.get(fsBase).resolve("testFile").toString());
    }

    private void testSetReadOnly(String fsBase) throws IOException {
        String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            FileUtils.setReadOnly(fileName);
            assertFalse(FileUtils.canWrite(fileName));
            FileUtils.delete(fileName);
        }
    }

    private void testSimple(final String fsBase) throws Exception {
        long time = System.currentTimeMillis();
        for (String s : FileUtils.newDirectoryStream(fsBase)) {
            FileUtils.delete(s);
        }
        FileUtils.createDirectories(fsBase + "/test");
        FileUtils.delete(fsBase + "/test");
        FileUtils.delete(fsBase + "/test2");
        assertTrue(FileUtils.createFile(fsBase + "/test"));
        List<FilePath> p = FilePath.get(fsBase).newDirectoryStream();
        assertEquals(1, p.size());
        String can = FilePath.get(fsBase + "/test").toRealPath().toString();
        assertEquals(can, p.get(0).toString());
        assertTrue(FileUtils.canWrite(fsBase + "/test"));
        FileChannel channel = FileUtils.open(fsBase + "/test", "rw");
        byte[] buffer = new byte[10000];
        Random random = new Random(1);
        random.nextBytes(buffer);
        channel.write(ByteBuffer.wrap(buffer));
        assertEquals(10000, channel.size());
        channel.position(20000);
        assertEquals(20000, channel.position());
        assertEquals(-1, channel.read(ByteBuffer.wrap(buffer, 0, 1)));
        String path = fsBase + "/test";
        assertEquals("test", FileUtils.getName(path));
        can = FilePath.get(fsBase).toRealPath().toString();
        String can2 = FileUtils.toRealPath(FileUtils.getParent(path));
        assertEquals(can, can2);
        FileLock lock = channel.tryLock();
        if (lock != null) {
            lock.release();
        }
        assertEquals(10000, channel.size());
        channel.close();
        assertEquals(10000, FileUtils.size(fsBase + "/test"));
        channel = FileUtils.open(fsBase + "/test", "r");
        final byte[] test = new byte[10000];
        FileUtils.readFully(channel, ByteBuffer.wrap(test, 0, 10000));
        IOUtilsTest.assertEquals(buffer, test);
        final FileChannel fc = channel;
        try {
            fc.write(ByteBuffer.wrap(test, 0, 10));
            fail();
        } catch (IOException e) {
            // expected
        }
        try {
            fc.truncate(10);
            fail();
        } catch (IOException e) {
            // expected
        }
        channel.close();
        long lastMod = FileUtils.lastModified(fsBase + "/test");
        if (lastMod < time - 1999) {
            // at most 2 seconds difference
            assertEquals(time, lastMod);
        }
        assertEquals(10000, FileUtils.size(fsBase + "/test"));
        List<String> list = FileUtils.newDirectoryStream(fsBase);
        assertEquals(1, list.size());
        assertTrue(list.get(0).endsWith("test"));
        FileUtils.copy(fsBase + "/test", fsBase + "/test3");
        FileUtils.moveTo(fsBase + "/test3", fsBase + "/test2");
        assertTrue(!FileUtils.exists(fsBase + "/test3"));
        assertTrue(FileUtils.exists(fsBase + "/test2"));
        assertEquals(10000, FileUtils.size(fsBase + "/test2"));
        byte[] buffer2 = new byte[10000];
        InputStream in = FileUtils.newInputStream(fsBase + "/test2");
        int pos = 0;
        while (true) {
            int l = in.read(buffer2, pos, Math.min(10000 - pos, 1000));
            if (l <= 0) {
                break;
            }
            pos += l;
        }
        in.close();
        assertEquals(10000, pos);
        IOUtilsTest.assertEquals(buffer, buffer2);

        assertTrue(FileUtils.tryDelete(fsBase + "/test2"));
        FileUtils.delete(fsBase + "/test");
        FileUtils.createDirectories(fsBase + "/testDir");
        assertTrue(FileUtils.isDirectory(fsBase + "/testDir"));
        if (!fsBase.startsWith("jdbc:")) {
            FileUtils.deleteRecursive(fsBase + "/testDir", false);
            assertTrue(!FileUtils.exists(fsBase + "/testDir"));
        }
    }

    private void testRandomAccess(String fsBase) throws Exception {
        testRandomAccess(fsBase, 1);
    }

    private void testRandomAccess(String fsBase, int seed) throws Exception {
        StringBuilder buff = new StringBuilder();
        String s = FileUtils.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        File file = new File(getBaseDir() + "/tmp");
        file.getParentFile().mkdirs();
        file.delete();
        RandomAccessFile ra = new RandomAccessFile(file, "rw");
        FileUtils.delete(s);
        FileChannel f = FileUtils.open(s, "rw");
        assertEquals(-1, f.read(ByteBuffer.wrap(new byte[1])));
        f.force(true);
        Random random = new Random(seed);
        int size = 500;
        try {
            for (int i = 0; i < size; i++) {
                trace("op " + i);
                int pos = random.nextInt(10000);
                switch(random.nextInt(7)) {
                case 0: {
                    pos = (int) Math.min(pos, ra.length());
                    trace("seek " + pos);
                    buff.append("seek " + pos + "\n");
                    f.position(pos);
                    ra.seek(pos);
                    break;
                }
                case 1: {
                    byte[] buffer = new byte[random.nextInt(1000)];
                    random.nextBytes(buffer);
                    trace("write " + buffer.length);
                    buff.append("write " + buffer.length + "\n");
                    f.write(ByteBuffer.wrap(buffer));
                    ra.write(buffer, 0, buffer.length);
                    break;
                }
                case 2: {
                    trace("truncate " + pos);
                    f.truncate(pos);
                    if (pos < ra.length()) {
                        // truncate is supposed to have no effect if the
                        // position is larger than the current size
                        ra.setLength(pos);
                    }
                    assertEquals("truncate " + pos, ra.getFilePointer(), f.position());
                    buff.append("truncate " + pos + "\n");
                    break;
                }
                case 3: {
                    int len = random.nextInt(1000);
                    len = (int) Math.min(len, ra.length() - ra.getFilePointer());
                    byte[] b1 = new byte[len];
                    byte[] b2 = new byte[len];
                    trace("readFully " + len);
                    ra.readFully(b1, 0, len);
                    FileUtils.readFully(f, ByteBuffer.wrap(b2, 0, len));
                    buff.append("readFully " + len + "\n");
                    IOUtilsTest.assertEquals(b1, b2);
                    break;
                }
                case 4: {
                    trace("getFilePointer " + ra.getFilePointer());
                    buff.append("getFilePointer " + ra.getFilePointer() + "\n");
                    assertEquals(ra.getFilePointer(), f.position());
                    break;
                }
                case 5: {
                    trace("length " + ra.length());
                    buff.append("length " + ra.length() + "\n");
                    assertEquals(ra.length(), f.size());
                    break;
                }
                case 6: {
                    trace("reopen");
                    buff.append("reopen\n");
                    f.close();
                    ra.close();
                    ra = new RandomAccessFile(file, "rw");
                    f = FileUtils.open(s, "rw");
                    assertEquals(ra.length(), f.size());
                    break;
                }
                default:
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Exception: " + e + "\n"+ buff.toString());
        } finally {
            f.close();
            ra.close();
            file.delete();
            FileUtils.delete(s);
        }
    }

    private void testTempFile(String fsBase) throws Exception {
        int len = 10000;
        String s = FileUtils.createTempFile(fsBase + "/tmp", ".tmp", false, false);
        OutputStream out = FileUtils.newOutputStream(s, false);
        byte[] buffer = new byte[len];
        out.write(buffer);
        out.close();
        out = FileUtils.newOutputStream(s, true);
        out.write(1);
        out.close();
        InputStream in = FileUtils.newInputStream(s);
        for (int i = 0; i < len; i++) {
            assertEquals(0, in.read());
        }
        assertEquals(1, in.read());
        assertEquals(-1, in.read());
        in.close();
        out.close();
        FileUtils.delete(s);
    }

    private void testUnsupportedFeatures(String fsBase) throws IOException {
        final String fileName = fsBase + "/testFile";
        if (FileUtils.exists(fileName)) {
            FileUtils.delete(fileName);
        }
        if (FileUtils.createFile(fileName)) {
            final FileChannel channel = FileUtils.open(fileName, "rw");
            try {
                channel.map(MapMode.PRIVATE, 0, channel.size());
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.read(ByteBuffer.allocate(10), 0);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.read(new ByteBuffer[]{ByteBuffer.allocate(10)}, 0, 0);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.write(ByteBuffer.allocate(10), 0);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.write(new ByteBuffer[]{ByteBuffer.allocate(10)}, 0, 0);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.transferFrom(channel, 0, 0);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            try {
                channel.transferTo(0, 0, channel);
                fail();
            } catch (UnsupportedOperationException e) {
                // expected
            }
            channel.close();
            FileUtils.delete(fileName);
        }
    }

    private void trace(String s) {
        // System.out.println(s);
    }

}
