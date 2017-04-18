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

package org.apache.jackrabbit.oak.plugins.value;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Blob implementation is based on an underlying {@link javax.jcr.Binary}.
 * <p>
 * Any error accessing the underlying binary in {@link #getNewStream()} will be
 * deferred to the returned input stream.
 */
public class BinaryBasedBlob implements Blob {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryBasedBlob.class);

    private final Binary binary;

    public BinaryBasedBlob(Binary binary) {
        this.binary = binary;
    }

    /**
     * Delegates to {@link Binary#getStream()} and returns an input stream the always
     * throws an {@code IOException} if the underlying binary failed to produce one.
     */
    @Nonnull
    @Override
    public InputStream getNewStream() {
        try {
            return binary.getStream();
        } catch (final RepositoryException e) {
            LOG.warn("Error retrieving stream from binary", e);
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    throw new IOException(e);
                }
            };
        }
    }

    /**
     * Delegates to {@link Binary#getSize()} and returns -1 if that fails.
     */
    @Override
    public long length() {
        try {
            return binary.getSize();
        } catch (RepositoryException e) {
            LOG.warn("Error determining length of binary", e);
            return -1;
        }
    }

    /**
     * @return  {@code null}
     */
    @Override
    public String getReference() {
        return null;
    }

    /**
     * @return  {@code null}
     */
    @Override
    public String getContentIdentity() {
        return null;
    }
}
