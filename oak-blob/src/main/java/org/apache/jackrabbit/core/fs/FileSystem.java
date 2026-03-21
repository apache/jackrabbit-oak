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
 * The <code>FileSystem</code> interface is an abstraction of a virtual
 * file system. The similarities of its method names with with the methods
 * of the <code>java.io.File</code> class are intentional.
 * <br>
 * Implementations of this interface expose a file system-like resource.
 * File system-like resources include WebDAV-enabled servers, local file systems,
 * and so forth.
 */
public interface FileSystem {

    /**
     * File separator
     */
    String SEPARATOR = "/";

    /**
     * File separator character
     */
    char SEPARATOR_CHAR = '/';

    /**
     * Initialize the file system
     *
     * @throws FileSystemException if the file system initialization fails
     */
    void init() throws FileSystemException;

    /**
     * Close the file system. After calling this method, the file system is no
     * longer accessible.
     *
     * @throws FileSystemException
     */
    void close() throws FileSystemException;

    /**
     * Returns an input stream of the contents of the file denoted by this path.
     *
     * @param filePath the path of the file.
     * @return an input stream of the contents of the file.
     * @throws FileSystemException if the file does not exist
     *                             or if it cannot be read from
     */
    InputStream getInputStream(String filePath) throws FileSystemException;

    /**
     * Returns an output stream for writing bytes to the file denoted by this path.
     * The file will be created if it doesn't exist. If the file exists, its contents
     * will be overwritten.
     *
     * @param filePath the path of the file.
     * @return an output stream for writing bytes to the file.
     * @throws FileSystemException if the file cannot be written to or created
     */
    OutputStream getOutputStream(String filePath) throws FileSystemException;

    /**
     * Creates the folder named by this path, including any necessary but
     * nonexistent parent folders. Note that if this operation fails it
     * may have succeeded in creating some of the necessary parent folders.
     *
     * @param folderPath the path of the folder to be created.
     * @throws FileSystemException if a file system entry denoted by path
     *                             already exists or if another error occurs.
     */
    void createFolder(String folderPath) throws FileSystemException;

    /**
     * Tests whether the file system entry denoted by this path exists.
     *
     * @param path the path of a file system entry.
     * @return true if the file system entry at path exists; false otherwise.
     * @throws FileSystemException
     */
    boolean exists(String path) throws FileSystemException;

    /**
     * Tests whether the file system entry denoted by this path exists and
     * is a file.
     *
     * @param path the path of a file system entry.
     * @return true if the file system entry at path is a file; false otherwise.
     * @throws FileSystemException
     */
    boolean isFile(String path) throws FileSystemException;

    /**
     * Tests whether the file system entry denoted by this path exists and
     * is a folder.
     *
     * @param path the path of a file system entry.
     * @return true if the file system entry at path is a folder; false otherwise.
     * @throws FileSystemException
     */
    boolean isFolder(String path) throws FileSystemException;

    /**
     * Tests whether the file system entry denoted by this path has child entries.
     *
     * @param path the path of a file system entry.
     * @return true if the file system entry at path has child entries; false otherwise.
     * @throws FileSystemException
     */
    boolean hasChildren(String path) throws FileSystemException;

    /**
     * Returns the length of the file denoted by this path.
     *
     * @param filePath the path of the file.
     * @return The length, in bytes, of the file denoted by this path,
     *         or -1L if the length can't be determined.
     * @throws FileSystemException if the path does not denote an existing file.
     */
    long length(String filePath) throws FileSystemException;

    /**
     * Returns the time that the file system entry denoted by this path
     * was last modified.
     *
     * @param path the path of a file system entry.
     * @return A long value representing the time the file system entry was
     *         last modified, measured in milliseconds since the epoch
     *         (00:00:00 GMT, January 1, 1970), or 0L if the modification
     *         time can't be determined.
     * @throws FileSystemException if the file system entry does not exist.
     */
    long lastModified(String path) throws FileSystemException;

    /**
     * Returns an array of strings naming the files and folders
     * in the folder denoted by this path.
     *
     * @param folderPath the path of the folder whose contents is to be listed.
     * @return an array of strings naming the files and folders
     *         in the folder denoted by this path.
     * @throws FileSystemException if this path does not denote a folder or if
     *                             another error occurs.
     */
    String[] list(String folderPath) throws FileSystemException;

    /**
     * Returns an array of strings naming the files in the folder
     * denoted by this path.
     *
     * @param folderPath the path of the folder whose contents is to be listed.
     * @return an array of strings naming the files in the folder
     *         denoted by this path.
     * @throws FileSystemException if this path does not denote a folder or if
     *                             another error occurs.
     */
    String[] listFiles(String folderPath) throws FileSystemException;

    /**
     * Returns an array of strings naming the folders in the folder
     * denoted by this path.
     *
     * @param folderPath the path of the folder whose contents is to be listed.
     * @return an array of strings naming the folders in the folder
     *         denoted by this path.
     * @throws FileSystemException if this path does not denote a folder or if
     *                             another error occurs.
     */
    String[] listFolders(String folderPath) throws FileSystemException;

    /**
     * Deletes the file denoted by this path.
     *
     * @param filePath the path of the file to be deleted.
     * @throws FileSystemException if this path does not denote a file or if
     *                             another error occurs.
     */
    void deleteFile(String filePath) throws FileSystemException;

    /**
     * Deletes the folder denoted by this path. Any contents of this folder
     * (folders and files) will be deleted recursively.
     *
     * @param folderPath the path of the folder to be deleted.
     * @throws FileSystemException if this path does not denote a folder or if
     *                             another error occurs.
     */
    void deleteFolder(String folderPath) throws FileSystemException;

}
