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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

/**
 * This file system stores files on disk.
 * This is the most common file system.
 */
public class FilePathDisk extends FilePath {

    private static final String CLASSPATH_PREFIX = "classpath:";
    private static final String FILE_SEPARATOR = System.getProperty("file.separator", "/");
    private static final int MAX_FILE_RETRY = 16;

    public FilePathDisk getPath(String path) {
        FilePathDisk p = new FilePathDisk();
        p.name = translateFileName(path);
        return p;
    }

    public long size() {
        return new File(name).length();
    }

    /**
     * Translate the file name to the native format. This will replace '\' with
     * '/' and expand the home directory ('~').
     *
     * @param fileName the file name
     * @return the native file name
     */
    protected static String translateFileName(String fileName) {
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("file:")) {
            fileName = fileName.substring("file:".length());
        }
        return expandUserHomeDirectory(fileName);
    }

    /**
     * Expand '~' to the user home directory. It is only be expanded if the '~'
     * stands alone, or is followed by '/' or '\'.
     *
     * @param fileName the file name
     * @return the native file name
     */
    public static String expandUserHomeDirectory(String fileName) {
        if (fileName.startsWith("~") && (fileName.length() == 1 || fileName.startsWith("~/"))) {
            String userDir = System.getProperty("user.home", "");
            fileName = userDir + fileName.substring(1);
        }
        return fileName;
    }

    public void moveTo(FilePath newName) throws IOException {
        File oldFile = new File(name);
        File newFile = new File(newName.name);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            return;
        }
        if (!oldFile.exists()) {
            throw new IOException("Could not rename " +
                    name + " (not found) to " + newName.name);
        }
        if (newFile.exists()) {
            throw new IOException("Could not rename " +
                    name + " to " + newName + " (already exists)");
        }
        for (int i = 0; i < MAX_FILE_RETRY; i++) {
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
        throw new IOException("Could not rename " + name + " to " + newName.name);
    }

    private static void wait(int i) {
        if (i == 8) {
            System.gc();
        }
        try {
            // sleep at most 256 ms
            long sleep = Math.min(256, i * i);
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public boolean createFile() {
        File file = new File(name);
        for (int i = 0; i < MAX_FILE_RETRY; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    public boolean exists() {
        return new File(name).exists();
    }

    public void delete() throws IOException {
        File file = new File(name);
        for (int i = 0; i < MAX_FILE_RETRY; i++) {
            boolean ok = file.delete();
            if (ok || !file.exists()) {
                return;
            }
            wait(i);
        }
        throw new IOException("Could not delete " + name);
    }

    public List<FilePath> newDirectoryStream() throws IOException {
        ArrayList<FilePath> list = new ArrayList<FilePath>();
        File f = new File(name);
        String[] files = f.list();
        if (files != null) {
            String base = f.getCanonicalPath();
            if (!base.endsWith(FILE_SEPARATOR)) {
                base += FILE_SEPARATOR;
            }
            for (int i = 0, len = files.length; i < len; i++) {
                list.add(getPath(base + files[i]));
            }
        }
        return list;
    }

    public boolean canWrite() {
        return canWriteInternal(new File(name));
    }

    public boolean setReadOnly() {
        File f = new File(name);
        return f.setReadOnly();
    }

    public FilePathDisk toRealPath() throws IOException {
        String fileName = new File(name).getCanonicalPath();
        return getPath(fileName);
    }

    public FilePath getParent() {
        String p = new File(name).getParent();
        return p == null ? null : getPath(p);
    }

    public boolean isDirectory() {
        return new File(name).isDirectory();
    }

    public boolean isAbsolute() {
        return new File(name).isAbsolute();
    }

    public long lastModified() {
        return new File(name).lastModified();
    }

    private static boolean canWriteInternal(File file) {
        try {
            if (!file.canWrite()) {
                return false;
            }
        } catch (Exception e) {
            // workaround for GAE which throws a
            // java.security.AccessControlException
            return false;
        }
        // File.canWrite() does not respect windows user permissions,
        // so we must try to open it using the mode "rw".
        // See also http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void createDirectory() throws IOException {
        File f = new File(name);
        if (f.exists()) {
            if (f.isDirectory()) {
                return;
            }
            throw new IOException("A file with this name already exists: " + name);
        }
        File dir = new File(name);
        for (int i = 0; i < MAX_FILE_RETRY; i++) {
            if ((dir.exists() && dir.isDirectory()) || dir.mkdir()) {
                return;
            }
            wait(i);
        }
        throw new IOException("Could not create " + name);
    }

    public OutputStream newOutputStream(boolean append) throws IOException {
        File file = new File(name);
        File parent = file.getParentFile();
        if (parent != null) {
            FileUtils.createDirectories(parent.getAbsolutePath());
        }
        FileOutputStream out = new FileOutputStream(name, append);
        return out;
    }

    public InputStream newInputStream() throws IOException {
        if (name.indexOf(':') > 1) {
            // if the : is in position 1, a windows file access is assumed: C:.. or D:
            if (name.startsWith(CLASSPATH_PREFIX)) {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                InputStream in = getClass().getResourceAsStream(fileName);
                if (in == null) {
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
                }
                if (in == null) {
                    throw new FileNotFoundException("Resource " + fileName);
                }
                return in;
            }
            // otherwise an URL is assumed
            URL url = new URL(name);
            InputStream in = url.openStream();
            return in;
        }
        FileInputStream in = new FileInputStream(name);
        return in;
    }

    /**
     * Call the garbage collection and run finalization. This close all files that
     * were not closed, and are no longer referenced.
     */
    static void freeMemoryAndFinalize() {
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    public FileChannel open(String mode) throws IOException {
        return new FileDisk(name, mode);
    }

    public String getScheme() {
        return "file";
    }

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        String fileName = name + ".";
        String prefix = new File(fileName).getName();
        File dir;
        if (inTempDir) {
            dir = new File(System.getProperty("java.io.tmpdir", "."));
        } else {
            dir = new File(fileName).getAbsoluteFile().getParentFile();
        }
        FileUtils.createDirectories(dir.getAbsolutePath());
        while (true) {
            File f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
            if (f.exists() || !f.createNewFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            if (deleteOnExit) {
                try {
                    f.deleteOnExit();
                } catch (Throwable e) {
                    // sometimes this throws a NullPointerException
                    // at java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                    // we can ignore it
                }
            }
            return get(f.getCanonicalPath());
        }
    }

    public FilePath resolve(String other) {
        return other == null ? this : getPath(name + "/" + other);
    }

}

/**
 * Uses java.io.RandomAccessFile to access a file.
 */
class FileDisk extends FileBase {

    private final RandomAccessFile file;
    private final String name;

    private long pos;

    FileDisk(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
        this.name = fileName;
    }

    public void force(boolean metaData) throws IOException {
        file.getFD().sync();
    }

    public FileChannel truncate(long newLength) throws IOException {
        if (newLength < file.length()) {
            // some implementations actually only support truncate
            file.setLength(newLength);
            pos = Math.min(pos, newLength);
        }
        return this;
    }

    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return file.getChannel().tryLock();
    }

    public void implCloseChannel() throws IOException {
        file.close();
    }

    public long position() throws IOException {
        return pos;
    }

    public long size() throws IOException {
        return file.length();
    }

    public int read(ByteBuffer dst) throws IOException {
        int len = file.read(dst.array(), dst.position(), dst.remaining());
        if (len > 0) {
            pos += len;
            dst.position(dst.position() + len);
        }
        return len;
    }

    public FileChannel position(long pos) throws IOException {
        if (this.pos != pos) {
            file.seek(pos);
            this.pos = pos;
        }
        return this;
    }

    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        file.write(src.array(), src.position(), len);
        src.position(src.position() + len);
        pos += len;
        return len;
    }

    public String toString() {
        return name;
    }

}
