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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.value.ErrorValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ValueFactory} interface.
 */
public class ValueFactoryImpl extends PartialValueFactory implements JackrabbitValueFactory {

    private static final PerfLogger binOpsLogger = new PerfLogger(
            LoggerFactory.getLogger("org.apache.jackrabbit.oak.jcr.operations.binary.perf"));

    @NotNull
    private final Root root;

    /**
     * Creates a new instance of {@code ValueFactory}.
     *
     * @param root the root instance for creating binary values
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * @param blobAccessProvider The blob access provider
     * the internal representation.
     */
    public ValueFactoryImpl(@NotNull Root root,
                            @NotNull NamePathMapper namePathMapper,
                            @NotNull BlobAccessProvider blobAccessProvider) {
        super(namePathMapper, blobAccessProvider);
        this.root = checkNotNull(root);
    }

    /**
     * Creates a new instance of {@code ValueFactory}. The {@link Value}s
     * created by this value factory instance will not be backed by a blob
     * access provider and never return a download URI for a binary value.
     *
     * @param root the root instance for creating binary values
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     */
    public ValueFactoryImpl(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        this(root, namePathMapper, DEFAULT_BLOB_ACCESS_PROVIDER);
    }

    //-------------------------------------------------------< ValueFactory >---

    @Override
    @NotNull
    public Value createValue(@NotNull InputStream value) {
        try {
            return createBinaryValue(value);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        } catch (RepositoryException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    @NotNull
    public Value createValue(@NotNull Binary value) {
        try {
            if (value instanceof BinaryImpl) {
                // No need to create the value again if we have it already underlying the binary
                return ((BinaryImpl) value).getBinaryValue();
            } else if (value instanceof ReferenceBinary) {
                String reference = ((ReferenceBinary) value).getReference();
                Blob blob = root.getBlob(reference);
                if (blob != null) {
                    return createBinaryValue(blob);
                }
            }
            InputStream stream = value.getStream();
            if (stream == null) {
                throw new ValueFormatException("null");
            }
            return createBinaryValue(stream);
        } catch (RepositoryException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    @NotNull
    public Binary createBinary(@NotNull InputStream stream) throws RepositoryException {
        try {
            return new BinaryImpl(createBinaryValue(stream));
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @Nullable
    public BinaryUpload initiateBinaryUpload(long maxSize, int maxParts) {
        BlobUpload upload = getBlobAccessProvider().initiateBlobUpload(maxSize, maxParts);
        if (null == upload) {
            return null;
        }

        return new BinaryUpload() {
            @Override
            @NotNull
            public Iterable<URI> getUploadURIs() {
                return upload.getUploadURIs();
            }

            @Override
            public long getMinPartSize() {
                return upload.getMinPartSize();
            }

            @Override
            public long getMaxPartSize() {
                return upload.getMaxPartSize();
            }

            @Override
            @NotNull
            public String getUploadToken() { return upload.getUploadToken(); }
        };
    }

    @Override
    @Nullable
    public Binary completeBinaryUpload(@NotNull String uploadToken) throws RepositoryException {
        return createBinary(
                getBlobAccessProvider().completeBlobUpload(uploadToken));
    }

    @NotNull
    private ValueImpl createBinaryValue(@NotNull InputStream value) throws IOException, RepositoryException {
        long start = binOpsLogger.start();
        Blob blob = root.createBlob(value);
        binOpsLogger.end(start, -1, "Created binary property of size [{}]", blob.length());
        return createBinaryValue(blob);
    }

    @NotNull
    private ValueImpl createBinaryValue(@NotNull Blob blob) throws RepositoryException {
        return new ValueImpl(BinaryPropertyState.binaryProperty("", blob),
                getNamePathMapper(), getBlobAccessProvider());
    }

    @Nullable
    public Binary createBinary(Blob blob) throws RepositoryException {
        return null != blob ? createBinaryValue(blob).getBinary() : null;
    }

    @Nullable
    public Blob getBlob(Binary binary) throws RepositoryException {
        if (binary instanceof BinaryImpl) {
            return ((BinaryImpl) binary).getBinaryValue().getBlob();
        }
        return null;
    }
}
