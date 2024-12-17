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
package org.apache.jackrabbit.oak.plugins.value.jcr;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.StringJoiner;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
class BinaryImpl implements ReferenceBinary, BinaryDownload {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryImpl.class);

    private final ValueImpl value;

    BinaryImpl(ValueImpl value) {
        this.value = value;
    }

    ValueImpl getBinaryValue() {
        return value.getType() == PropertyType.BINARY ? value : null;
    }

    //-------------------------------------------------------------< Binary >---

    @Override
    public InputStream getStream() throws RepositoryException {
        return value.getBlob().getNewStream();
    }

    @Override
    public int read(byte[] b, long position) throws IOException, RepositoryException {
        InputStream stream = getStream();
        try {
            if (position != stream.skip(position)) {
                throw new IOException("Can't skip to position " + position);
            }
            return stream.read(b);
        } finally {
            stream.close();
        }
    }

    @Override
    public long getSize() throws RepositoryException {
        switch (value.getType()) {
        case PropertyType.NAME:
        case PropertyType.PATH:
            // need to respect namespace remapping
            return value.getString().length();
        default:
            return value.getBlob().length();
        }
    }

    @Override
    public void dispose() {
        // nothing to do
    }

    @Nullable
    @Override
    public URI getURI(@NotNull BinaryDownloadOptions downloadOptions)
            throws RepositoryException {
        ValueImpl v = getBinaryValue();
        if (v == null || v.getBlob().isInlined()) {
            // property is not a binary, or it is inlined
            // we cannot return a URI for it
            return null;
        }

        // ValueFactoryImpl.getBlobId() will only return a blobId for a BinaryImpl, which
        // a client cannot spoof, so we know that the id in question is valid and can be
        // trusted, so we can safely give out a URI to the binary for downloading.
        Blob blob = v.getBlob();
        String blobId = blob.getContentIdentity();
        if (null != blobId) {
            return v.getDownloadURI(blob, downloadOptions);
        }
        return null;
    }

    //---------------------------------------------------< ReferenceBinary >--

    @Override @Nullable
    public String getReference() {
        try {
            return value.getBlob().getReference();
        } catch (RepositoryException e) {
            LOG.warn("Error getting binary reference", e);
            return null;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ReferenceBinary) {
            return Objects.equals(getReference(), ((ReferenceBinary) other).getReference());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getReference());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BinaryImpl.class.getSimpleName() + "[", "]")
                .add("value=" + value)
                .toString();
    }
}
