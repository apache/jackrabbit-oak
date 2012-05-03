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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * The base class for wrapping / delegating file systems such as
 * the split file system.
 */
public abstract class FilePathWrapper extends FilePath {

    private FilePath base;

    @Override
    public FilePathWrapper getPath(String path) {
        return create(path, unwrap(path));
    }

    /**
     * Create a wrapped path instance for the given base path.
     *
     * @param base the base path
     * @return the wrapped path
     */
    public FilePathWrapper wrap(FilePath base) {
        return base == null ? null : create(getPrefix() + base.name, base);
    }

    public FilePath unwrap() {
        return unwrap(name);
    }

    private FilePathWrapper create(String path, FilePath base) {
        try {
            FilePathWrapper p = getClass().newInstance();
            p.name = path;
            p.base = base;
            return p;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected String getPrefix() {
        return getScheme() + ":";
    }

    /**
     * Get the base path for the given wrapped path.
     *
     * @param path the path including the scheme prefix
     * @return the base file path
     */
    protected FilePath unwrap(String path) {
        return FilePath.get(path.substring(getScheme().length() + 1));
    }

    protected FilePath getBase() {
        return base;
    }

    @Override
    public boolean canWrite() {
        return base.canWrite();
    }

    @Override
    public void createDirectory() throws IOException {
        base.createDirectory();
    }

    @Override
    public boolean createFile() {
        return base.createFile();
    }

    @Override
    public void delete() throws IOException {
        base.delete();
    }

    @Override
    public boolean exists() {
        return base.exists();
    }

    @Override
    public FilePath getParent() {
        return wrap(base.getParent());
    }

    @Override
    public boolean isAbsolute() {
        return base.isAbsolute();
    }

    @Override
    public boolean isDirectory() {
        return base.isDirectory();
    }

    @Override
    public long lastModified() {
        return base.lastModified();
    }

    @Override
    public FilePath toRealPath() throws IOException {
        return wrap(base.toRealPath());
    }

    @Override
    public List<FilePath> newDirectoryStream() throws IOException {
        List<FilePath> list = base.newDirectoryStream();
        for (int i = 0, len = list.size(); i < len; i++) {
            list.set(i, wrap(list.get(i)));
        }
        return list;
    }

    @Override
    public void moveTo(FilePath newName) throws IOException {
        base.moveTo(((FilePathWrapper) newName).base);
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return base.newInputStream();
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return base.newOutputStream(append);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return base.open(mode);
    }

    @Override
    public boolean setReadOnly() {
        return base.setReadOnly();
    }

    @Override
    public long size() {
        return base.size();
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return wrap(base.createTempFile(suffix, deleteOnExit, inTempDir));
    }

    @Override
    public FilePath resolve(String other) {
        return other == null ? this : wrap(base.resolve(other));
    }

}
