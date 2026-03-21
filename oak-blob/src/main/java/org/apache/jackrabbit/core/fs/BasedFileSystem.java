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
package org.apache.jackrabbit.core.fs;

import java.io.InputStream;
import java.io.OutputStream;


/**
 * A <code>BasedFileSystem</code> represents a 'file system in a file system'.
 */
public class BasedFileSystem implements FileSystem {

    protected final FileSystem fsBase;

    protected final String basePath;

    /**
     * Creates a new <code>BasedFileSystem</code>
     *
     * @param fsBase      the <code>FileSystem</code> the new file system should be based on
     * @param relRootPath the root path relative to <code>fsBase</code>'s root
     */
    public BasedFileSystem(FileSystem fsBase, String relRootPath) {
        if (fsBase == null) {
            throw new IllegalArgumentException("invalid file system argument");
        }
        this.fsBase = fsBase;

        if (relRootPath == null) {
            throw new IllegalArgumentException("invalid null path argument");
        }
        if (relRootPath.equals(SEPARATOR)) {
            throw new IllegalArgumentException("invalid path argument");
        }
        if (!relRootPath.startsWith(SEPARATOR)) {
            relRootPath = SEPARATOR + relRootPath;
        }
        if (relRootPath.endsWith(SEPARATOR)) {
            relRootPath = relRootPath.substring(0, relRootPath.length() - 1);

        }
        this.basePath = relRootPath;
    }

    protected String buildBasePath(String path) {
        if (path.startsWith(SEPARATOR)) {
            if (path.length() == 1) {
                return basePath;
            } else {
                return basePath + path;
            }
        } else {
            return basePath + SEPARATOR + path;
        }
    }

    //-----------------------------------------------------------< FileSystem >
    /**
     * {@inheritDoc}
     */
    public void init() throws FileSystemException {
        // check base path
        if (!fsBase.isFolder(basePath)) {
            fsBase.createFolder(basePath);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws FileSystemException {
        // do nothing; base file system should be closed explicitly
    }

    /**
     * {@inheritDoc}
     */
    public void createFolder(String folderPath) throws FileSystemException {
        fsBase.createFolder(buildBasePath(folderPath));
    }

    /**
     * {@inheritDoc}
     */
    public void deleteFile(String filePath) throws FileSystemException {
        fsBase.deleteFile(buildBasePath(filePath));
    }

    /**
     * {@inheritDoc}
     */
    public void deleteFolder(String folderPath) throws FileSystemException {
        fsBase.deleteFolder(buildBasePath(folderPath));
    }

    /**
     * {@inheritDoc}
     */
    public boolean exists(String path) throws FileSystemException {
        return fsBase.exists(buildBasePath(path));
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getInputStream(String filePath) throws FileSystemException {
        return fsBase.getInputStream(buildBasePath(filePath));
    }

    /**
     * {@inheritDoc}
     */
    public OutputStream getOutputStream(String filePath) throws FileSystemException {
        return fsBase.getOutputStream(buildBasePath(filePath));
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasChildren(String path) throws FileSystemException {
        return fsBase.hasChildren(buildBasePath(path));
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFile(String path) throws FileSystemException {
        return fsBase.isFile(buildBasePath(path));
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFolder(String path) throws FileSystemException {
        return fsBase.isFolder(buildBasePath(path));
    }

    /**
     * {@inheritDoc}
     */
    public long lastModified(String path) throws FileSystemException {
        return fsBase.lastModified(buildBasePath(path));
    }

    /**
     * {@inheritDoc}
     */
    public long length(String filePath) throws FileSystemException {
        return fsBase.length(buildBasePath(filePath));
    }

    /**
     * {@inheritDoc}
     */
    public String[] list(String folderPath) throws FileSystemException {
        return fsBase.list(buildBasePath(folderPath));
    }

    /**
     * {@inheritDoc}
     */
    public String[] listFiles(String folderPath) throws FileSystemException {
        return fsBase.listFiles(buildBasePath(folderPath));
    }

    /**
     * {@inheritDoc}
     */
    public String[] listFolders(String folderPath) throws FileSystemException {
        return fsBase.listFolders(buildBasePath(folderPath));
    }
}
