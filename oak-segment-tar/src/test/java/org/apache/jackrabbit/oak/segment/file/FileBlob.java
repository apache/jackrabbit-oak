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
package org.apache.jackrabbit.oak.segment.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;

/**
 * A blob as a file in the file system.
 * Used for testing.
 */
public class FileBlob implements Blob {

    private final String path;

    public FileBlob(String path) {
        this.path = path;
    }

    @Override
    public String getReference() {
        return path; // FIXME: should be a secure reference
    }

    @Override
    public String getContentIdentity() {
        return null;
    }

    @Nonnull
    @Override
    public InputStream getNewStream() {
        try {
            return new FileInputStream(getFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long length() {
        return getFile().length();
    }

    private File getFile() {
        return new File(path);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileBlob) {
            FileBlob other = (FileBlob) obj;
            return this.path.equals(other.path);
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
