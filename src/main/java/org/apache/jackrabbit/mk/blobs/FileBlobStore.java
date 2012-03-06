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
package org.apache.jackrabbit.mk.blobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import org.apache.jackrabbit.mk.fs.FilePath;
import org.apache.jackrabbit.mk.fs.FileUtils;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.StringUtils;

/**
 * A file blob store.
 */
public class FileBlobStore extends AbstractBlobStore {

    private static final String OLD_SUFFIX = "_old";

    private final FilePath baseDir;
    private final byte[] buffer = new byte[16 * 1024];
    private boolean mark;

    public FileBlobStore(String dir) throws IOException {
        baseDir = FilePath.get(dir);
        FileUtils.createDirectories(dir);
    }

    @Override
    public String addBlob(String tempFilePath) {
        try {
            FilePath file = FilePath.get(tempFilePath);
            InputStream in = file.newInputStream();
            MessageDigest messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
            DigestInputStream din = new DigestInputStream(in, messageDigest);
            long length = file.size();
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
            FilePath f = getFile(digest, false);
            if (f.exists()) {
                file.delete();
            } else {
                FilePath parent = f.getParent();
                if (!parent.exists()) {
                    FileUtils.createDirectories(parent.toString());
                }
                file.moveTo(f);
            }
            IOUtils.writeVarInt(idStream, digest.length);
            idStream.write(digest);
            byte[] id = idStream.toByteArray();
            String blobId = StringUtils.convertBytesToHex(id);
            usesBlobId(blobId);
            return blobId;
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    @Override
    protected synchronized void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        FilePath f = getFile(digest, false);
        if (f.exists()) {
            return;
        }
        FilePath parent = f.getParent();
        if (!parent.exists()) {
            FileUtils.createDirectories(parent.toString());
        }
        FilePath temp = parent.resolve(f.getName() + ".temp");
        OutputStream out = temp.newOutputStream(false);
        out.write(data);
        out.close();
        temp.moveTo(f);
    }

    private FilePath getFile(byte[] digest, boolean old) {
        String id = StringUtils.convertBytesToHex(digest);
        String sub = id.substring(id.length() - 2);
        if (old) {
            sub += OLD_SUFFIX;
        }
        return baseDir.resolve(sub).resolve(id + ".dat");
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId id) throws IOException {
        FilePath f = getFile(id.digest, false);
        if (!f.exists()) {
            FilePath old = getFile(id.digest, true);
            f.getParent().createDirectory();
            old.moveTo(f);
            f = getFile(id.digest, false);
        }
        int length = (int) Math.min(f.size(), getBlockSize());
        byte[] data = new byte[length];
        InputStream in = f.newInputStream();
        try {
            IOUtils.skipFully(in, id.pos);
            IOUtils.readFully(in, data, 0, length);
        } finally {
            in.close();
        }
        return data;
    }

    @Override
    public void startMark() throws Exception {
        mark = true;
        for (int i = 0; i < 256; i++) {
            String sub = StringUtils.convertBytesToHex(new byte[] { (byte) i });
            FilePath d = baseDir.resolve(sub);
            FilePath old = baseDir.resolve(sub + OLD_SUFFIX);
            if (d.exists()) {
                if (old.exists()) {
                    for (FilePath p : d.newDirectoryStream()) {
                        String name = p.getName();
                        FilePath newName = old.resolve(name);
                        p.moveTo(newName);
                    }
                } else {
                    d.moveTo(old);
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
        FilePath f = getFile(id.digest, false);
        if (!f.exists()) {
            FilePath old = getFile(id.digest, true);
            f.getParent().createDirectory();
            old.moveTo(f);
            f = getFile(id.digest, false);
        }
    }

    @Override
    public int sweep() throws IOException {
        int count = 0;
        for (int i = 0; i < 256; i++) {
            String sub = StringUtils.convertBytesToHex(new byte[] { (byte) i });
            FilePath old = baseDir.resolve(sub + OLD_SUFFIX);
            if (old.exists()) {
                for (FilePath p : old.newDirectoryStream()) {
                    String name = p.getName();
                    FilePath file = old.resolve(name);
                    file.delete();
                    count++;
                }
                old.delete();
            }
        }
        mark = false;
        return count;
    }

}