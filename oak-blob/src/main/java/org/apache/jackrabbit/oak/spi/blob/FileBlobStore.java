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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.StringUtils;

/**
 * A file blob store.
 */
public class FileBlobStore extends AbstractBlobStore {

    private static final String OLD_SUFFIX = "_old";
    private static final String FILE_SUFFIX = ".dat";

    private final File baseDir;
    private final byte[] buffer = new byte[16 * 1024];
    private boolean mark;

    // TODO file operations are not secure (return values not checked, no retry,...)

    public FileBlobStore(String dir) {
        baseDir = new File(dir);
        baseDir.mkdirs();
    }

    @Override
    public String writeBlob(String tempFilePath) throws IOException {
        File file = new File(tempFilePath);
        InputStream in = new FileInputStream(file);
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        DigestInputStream din = new DigestInputStream(in, messageDigest);
        long length = file.length();
        try {
            while (true) {
                int len = din.read(buffer, 0, buffer.length);
                if (len < 0) {
                    break;
                }
            }
        } finally {
            din.close();
        }
        ByteArrayOutputStream idStream = new ByteArrayOutputStream();
        idStream.write(TYPE_HASH);
        IOUtils.writeVarInt(idStream, 0);
        IOUtils.writeVarLong(idStream, length);
        byte[] digest = messageDigest.digest();
        File f = getFile(digest, false);
        if (f.exists()) {
            file.delete();
        } else {
            File parent = f.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            file.renameTo(f);
        }
        IOUtils.writeVarInt(idStream, digest.length);
        idStream.write(digest);
        byte[] id = idStream.toByteArray();
        String blobId = StringUtils.convertBytesToHex(id);
        usesBlobId(blobId);
        return blobId;
    }

    @Override
    protected synchronized void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        File f = getFile(digest, false);
        if (f.exists()) {
            return;
        }
        File parent = f.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        File temp = new File(parent, f.getName() + ".temp");
        OutputStream out = new FileOutputStream(temp, false);
        out.write(data);
        out.close();
        temp.renameTo(f);
    }

    private File getFile(byte[] digest, boolean old) {
        String id = StringUtils.convertBytesToHex(digest);
        String sub1 = id.substring(id.length() - 2);
        String sub2 = id.substring(id.length() - 4, id.length() - 2);
        if (old) {
            sub2 += OLD_SUFFIX;
        }
        return new File(new File(new File(baseDir, sub1), sub2), id + FILE_SUFFIX);
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId id) throws IOException {
        File f = getFile(id.digest, false);
        if (!f.exists()) {
            File old = getFile(id.digest, true);
            f.getParentFile().mkdir();
            old.renameTo(f);
            f = getFile(id.digest, false);
        }
        int length = (int) Math.min(f.length(), getBlockSize());
        byte[] data = new byte[length];
        InputStream in = new FileInputStream(f);
        try {
            IOUtils.skipFully(in, id.pos);
            IOUtils.readFully(in, data, 0, length);
        } finally {
            in.close();
        }
        return data;
    }

    @Override
    public void startMark() throws IOException {
        mark = true;
        for (int j = 0; j < 256; j++) {
            String sub1 = StringUtils.convertBytesToHex(new byte[] { (byte) j });
            File x = new File(baseDir, sub1);
            for (int i = 0; i < 256; i++) {
                String sub2 = StringUtils.convertBytesToHex(new byte[] { (byte) i });
                File d = new File(x, sub2);
                File old = new File(x, sub2 + OLD_SUFFIX);
                if (d.exists()) {
                    if (old.exists()) {
                        for (File p : d.listFiles()) {
                            String name = p.getName();
                            File newName = new File(old, name);
                            p.renameTo(newName);
                        }
                    } else {
                        d.renameTo(old);
                    }
                }
            }
        }
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return mark;
    }

    @Override
    protected void mark(BlockId id) throws IOException {
        File f = getFile(id.digest, false);
        if (!f.exists()) {
            File old = getFile(id.digest, true);
            f.getParentFile().mkdir();
            old.renameTo(f);
            f = getFile(id.digest, false);
        }
    }

    @Override
    public int sweep() throws IOException {
        int count = 0;
        for (int j = 0; j < 256; j++) {
            String sub1 = StringUtils.convertBytesToHex(new byte[] { (byte) j });
            File x = new File(baseDir, sub1);
            for (int i = 0; i < 256; i++) {
                String sub = StringUtils.convertBytesToHex(new byte[] { (byte) i });
                File old = new File(x, sub + OLD_SUFFIX);
                if (old.exists()) {
                    for (File p : old.listFiles()) {
                        String name = p.getName();
                        File file = new File(old, name);
                        file.delete();
                        count++;
                    }
                    old.delete();
                }
            }
        }
        mark = false;
        return count;
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        int count = 0;
        for (String chunkId : chunkIds) {
            byte[] digest = StringUtils.convertHexToBytes(chunkId);
            File f = getFile(digest, false);
            if (!f.exists()) {
                File old = getFile(digest, true);
                f.getParentFile().mkdir();
                old.renameTo(f);
                f = getFile(digest, false);
            }
            if ((maxLastModifiedTime <= 0) 
                    || FileUtils.isFileOlder(f, maxLastModifiedTime)) {
                f.delete();
                count++;
            }
        }
        return count;
    }

    @Override
    public Iterator<String> getAllChunkIds(final long maxLastModifiedTime) throws Exception {
        FluentIterable<File> iterable = Files.fileTreeTraverser().postOrderTraversal(baseDir);
        final Iterator<File> iter =
                iterable.filter(new Predicate<File>() {
                    // Ignore the directories and files newer than maxLastModifiedTime if specified
                    @Override
                    public boolean apply(@Nullable File input) {
                        if (!input.isDirectory() && (
                                (maxLastModifiedTime <= 0)
                                    || FileUtils.isFileOlder(input, maxLastModifiedTime))) {
                            return true;
                        }
                        return false;
                    }
                }).iterator();
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (iter.hasNext()) {
                    File file = iter.next();
                    return FilenameUtils.removeExtension(file.getName());
                }
                return endOfData();
            }
        };
    }

    @Override
    public void clearCache() {
        // no cache
    }
}
