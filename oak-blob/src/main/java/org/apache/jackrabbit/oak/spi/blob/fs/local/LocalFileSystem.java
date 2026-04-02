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
// copied from Apache Jackrabbit jackrabbit-data module; original class org.apache.jackrabbit.core.fs.local.LocalFileSystem
package org.apache.jackrabbit.oak.spi.blob.fs.local;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.blob.fs.FileSystem;
import org.apache.jackrabbit.oak.spi.blob.fs.FileSystemException;
import org.apache.jackrabbit.util.LazyFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A <code>LocalFileSystem</code> ...
 */
public class LocalFileSystem implements FileSystem {

    private static Logger log = LoggerFactory.getLogger(LocalFileSystem.class);

    private File root;

    /**
     * Default constructor
     */
    public LocalFileSystem() {
    }

    public String getPath() {
        if (root != null) {
            return root.getPath();
        } else {
            return null;
        }
    }

    /**
     * Sets the path to the root directory of this local filesystem. please note
     * that this method can be called via reflection during initialization and
     * must not be altered.
     *
     * @param rootPath the path to the root directory
     */
    public void setPath(String rootPath) {
        setRoot(new File(osPath(rootPath)));
    }

    public void setRoot(File root) {
        this.root = root;
    }

    private String osPath(String genericPath) {
        if (File.separator.equals(SEPARATOR)) {
            return genericPath;
        }
        return genericPath.replace(SEPARATOR_CHAR, File.separatorChar);
    }

    //-------------------------------------------< java.lang.Object overrides >
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof LocalFileSystem) {
            LocalFileSystem other = (LocalFileSystem) obj;
            if (root == null) {
                return other.root == null;
            } else {
                return root.equals(other.root);
            }
        }
        return false;
    }

    /**
     * Returns zero to satisfy the Object equals/hashCode contract.
     * This class is mutable and not meant to be used as a hash key.
     *
     * @return always zero
     * @see Object#hashCode()
     */
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< FileSystem >
    /**
     * {@inheritDoc}
     */
    public void init() throws FileSystemException {
        if (root == null) {
            String msg = "root directory not set";
            log.debug(msg);
            throw new FileSystemException(msg);
        }

        if (root.exists()) {
            if (!root.isDirectory()) {
                String msg = "path does not denote a folder";
                log.debug(msg);
                throw new FileSystemException(msg);
            }
        } else {
            if (!root.mkdirs()) {
                String msg = "failed to create root";
                log.debug(msg);
                throw new FileSystemException(msg);
            }
        }
        log.info("LocalFileSystem initialized at path " + root.getPath());
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws FileSystemException {
        root = null;
    }

    /**
     * {@inheritDoc}
     */
    public void deleteFile(String filePath) throws FileSystemException {
        File f = new File(root, osPath(filePath));
        if (!f.isFile()) {
            String msg = f.getPath() + " does not denote an existing file";
            throw new FileSystemException(msg);
        }
        try {
            FileUtils.forceDelete(f);
        } catch (IOException ioe) {
            String msg = "failed to delete " + f.getPath();
            throw new FileSystemException(msg, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void deleteFolder(String folderPath) throws FileSystemException {
        File f = new File(root, osPath(folderPath));
        if (!f.isDirectory()) {
            String msg = f.getPath() + " does not denote an existing folder";
            log.debug(msg);
            throw new FileSystemException(msg);
        }
        try {
            FileUtils.forceDelete(f);
        } catch (IOException ioe) {
            String msg = "failed to delete " + f.getPath();
            log.debug(msg);
            throw new FileSystemException(msg, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean exists(String path) throws FileSystemException {
        File f = new File(root, osPath(path));
        return f.exists();
    }

    /**
     * {@inheritDoc}
     */
    public InputStream getInputStream(String filePath)
            throws FileSystemException {
        File f = new File(root, osPath(filePath));
        try {
            return new LazyFileInputStream(f);
        } catch (FileNotFoundException fnfe) {
            String msg = f.getPath() + " does not denote an existing file";
            log.debug(msg);
            throw new FileSystemException(msg, fnfe);
        }
    }

    /**
     * {@inheritDoc}
     */
    public OutputStream getOutputStream(String filePath)
            throws FileSystemException {
        File f = new File(root, osPath(filePath));
        try {
            return new FileOutputStream(f);
        } catch (FileNotFoundException fnfe) {
            String msg = "failed to get output stream for " + f.getPath();
            log.debug(msg);
            throw new FileSystemException(msg, fnfe);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasChildren(String path) throws FileSystemException {
        File f = new File(root, osPath(path));
        if (!f.exists()) {
            String msg = f.getPath() + " does not exist";
            log.debug(msg);
            throw new FileSystemException(msg);
        }
        if (f.isFile()) {
            return false;
        }
        return (f.list().length > 0);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFile(String path) throws FileSystemException {
        File f = new File(root, osPath(path));
        return f.isFile();
    }

    /**
     * {@inheritDoc}
     */
    public long lastModified(String path) throws FileSystemException {
        File f = new File(root, osPath(path));
        return f.lastModified();
    }

    /**
     * {@inheritDoc}
     */
    public long length(String filePath) throws FileSystemException {
        File f = new File(root, osPath(filePath));
        if (!f.exists()) {
            return -1;
        }
        return f.length();
    }

    /**
     * {@inheritDoc}
     */
    public String[] list(String folderPath) throws FileSystemException {
        File f = new File(root, osPath(folderPath));
        String[] entries = f.list();
        if (entries == null) {
            String msg = folderPath + " does not denote a folder";
            log.debug(msg);
            throw new FileSystemException(msg);
        }
        return entries;
    }

    /**
     * {@inheritDoc}
     */
    public String[] listFiles(String folderPath) throws FileSystemException {
        File folder = new File(root, osPath(folderPath));
        File[] files = folder.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return f.isFile();
            }
        });
        if (files == null) {
            String msg = folderPath + " does not denote a folder";
            log.debug(msg);
            throw new FileSystemException(msg);
        }
        String[] entries = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            entries[i] = files[i].getName();
        }
        return entries;
    }
}
