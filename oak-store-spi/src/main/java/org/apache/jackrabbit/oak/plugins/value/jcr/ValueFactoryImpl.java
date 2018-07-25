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
import static org.apache.jackrabbit.oak.plugins.value.jcr.ValueImpl.newValue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.List;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.NodeType;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.ErrorValue;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ValueFactory} interface.
 */
public class ValueFactoryImpl implements JackrabbitValueFactory {
    private static final PerfLogger binOpsLogger = new PerfLogger(
            LoggerFactory.getLogger("org.apache.jackrabbit.oak.jcr.operations.binary.perf"));
    private final Root root;
    private final NamePathMapper namePathMapper;

    @NotNull
    private final BlobAccessProvider blobAccessProvider;

    public ValueFactoryImpl(@NotNull Root root, @NotNull NamePathMapper namePathMapper,
                            @Nullable BlobAccessProvider blobAccessProvider) {
        this.root = checkNotNull(root);
        this.namePathMapper = checkNotNull(namePathMapper);
        this.blobAccessProvider = blobAccessProvider == null
                ? new DefaultBlobAccessProvider()
                : blobAccessProvider;
    }

    /**
     * Creates a new instance of {@code ValueFactory}.
     *
     * @param root the root instance for creating binary values
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     */
    public ValueFactoryImpl(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        this(root, namePathMapper, null);
    }

    /**
     * Utility method for creating a {@code Value} based on a {@code PropertyState}.
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    public static Value createValue(PropertyState property, NamePathMapper namePathMapper) {
        return newValue(property, namePathMapper);
    }

    /**
     * Utility method for creating a {@code Value} based on a {@code PropertyValue}.
     * @param property  The property value
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     */
    @NotNull
    public static Value createValue(@NotNull PropertyValue property, @NotNull NamePathMapper namePathMapper) {
        PropertyState ps = PropertyValues.create(property);
        if (ps == null) {
            throw new IllegalArgumentException("Failed to convert the specified property value to a property state.");
        }
        return newValue(ps, namePathMapper);
    }

    /**
     * Utility method for creating {@code Value}s based on a {@code PropertyState}.
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  A list of new {@code Value} instances
     */
    public static List<Value> createValues(PropertyState property, NamePathMapper namePathMapper) {
        List<Value> values = Lists.newArrayList();
        for (int i = 0; i < property.count(); i++) {
            values.add(newValue(property, i, namePathMapper));
        }
        return values;
    }

    /**
     * Utility method for creating {@code Value}s based on a {@code PropertyState}.
     * @param property  The property state
     * @return  A list of new {@code Value} instances
     */
    public List<Value> createValues(PropertyState property) {
        List<Value> values = Lists.newArrayList();
        for (int i = 0; i < property.count(); i++) {
            values.add(newValue(property, i, namePathMapper));
        }
        return values;
    }

    //-------------------------------------------------------< ValueFactory >---

    @Override
    public Value createValue(String value) {
        return newValue(StringPropertyState.stringProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(InputStream value) {
        try {
            return createBinaryValue(value);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        } catch (RepositoryException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    public Value createValue(Binary value) {
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
            return createBinaryValue(value.getStream());
        } catch (RepositoryException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        } catch (IOException e) {
            return new ErrorValue(e, PropertyType.BINARY);
        }
    }

    @Override
    public Value createValue(long value) {
        return newValue(LongPropertyState.createLongProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(double value) {
        return newValue(DoublePropertyState.doubleProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Calendar value) {
        return newValue(PropertyStates.createProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(boolean value) {
        return newValue(BooleanPropertyState.booleanProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @Override @SuppressWarnings("deprecation")
    public Value createValue(Node value, boolean weak) throws RepositoryException {
        if (!value.isNodeType(NodeType.MIX_REFERENCEABLE)) {
            throw new ValueFormatException(
                    "Node is not referenceable: " + value.getPath());
        }
        return weak
            ? newValue(GenericPropertyState.weakreferenceProperty("", value.getUUID()), namePathMapper)
            : newValue(GenericPropertyState.referenceProperty("", value.getUUID()), namePathMapper);
    }

    @Override
    public Value createValue(BigDecimal value) {
        return newValue(DecimalPropertyState.decimalProperty("", value), namePathMapper);
    }

    @Override
    public Value createValue(String value, int type) throws ValueFormatException {
        if (value == null) {
            throw new ValueFormatException("null");
        }

        try {
            switch (type) {
                case PropertyType.STRING:
                    return createValue(value);
                case PropertyType.BINARY:
                    return newValue(BinaryPropertyState.binaryProperty("", value), namePathMapper);
                case PropertyType.LONG:
                    return createValue(Conversions.convert(value).toLong());
                case PropertyType.DOUBLE:
                    return createValue(Conversions.convert(value).toDouble());
                case PropertyType.DATE:
                    if (ISO8601.parse(value) == null) {
                        throw new ValueFormatException("Invalid date " + value);
                    }
                    return newValue(GenericPropertyState.dateProperty("", value), namePathMapper);
                case PropertyType.BOOLEAN:
                    return createValue(Conversions.convert(value).toBoolean());
                case PropertyType.NAME:
                    String oakName = namePathMapper.getOakNameOrNull(value);
                    if (oakName == null || !JcrNameParser.validate(oakName)) {
                        throw new ValueFormatException("Invalid name: " + value);
                    }
                    return newValue(GenericPropertyState.nameProperty("", oakName), namePathMapper);
                case PropertyType.PATH:
                    String oakValue = value;
                    if (value.startsWith("[") && value.endsWith("]")) {
                        // identifier path; do no change
                    } else {
                        oakValue = namePathMapper.getOakPath(value);
                        if (oakValue == null || !JcrPathParser.validate(oakValue)) {
                            throw new ValueFormatException("Invalid path: " + value);
                        }
                    }
                    return newValue(GenericPropertyState.pathProperty("", oakValue), namePathMapper);
                case PropertyType.REFERENCE:
                    if (!UUIDUtils.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid reference value " + value);
                    }
                    return newValue(GenericPropertyState.referenceProperty("", value), namePathMapper);
                case PropertyType.WEAKREFERENCE:
                    if (!UUIDUtils.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid weak reference value " + value);
                    }
                    return newValue(GenericPropertyState.weakreferenceProperty("", value), namePathMapper);
                case PropertyType.URI:
                    new URI(value);
                    return newValue(GenericPropertyState.uriProperty("", value), namePathMapper);
                case PropertyType.DECIMAL:
                    return createValue(Conversions.convert(value).toDecimal());
                default:
                    throw new ValueFormatException("Invalid type: " + type);
            }
        } catch (NumberFormatException | URISyntaxException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        }
    }

    @Override
    public Binary createBinary(InputStream stream) throws RepositoryException {
        try {
            return new BinaryImpl(createBinaryValue(stream));
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    @Nullable
    public BinaryUpload initiateBinaryUpload(long maxSize, int maxParts) {
        BlobUpload upload = blobAccessProvider.initiateBlobUpload(maxSize, maxParts);
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
                blobAccessProvider.completeBlobUpload(uploadToken));
    }

    private ValueImpl createBinaryValue(InputStream value) throws IOException, RepositoryException {
        long start = binOpsLogger.start();
        Blob blob = root.createBlob(value);
        binOpsLogger.end(start, -1, "Created binary property of size [{}]", blob.length());
        return createBinaryValue(blob);
    }

    private ValueImpl createBinaryValue(Blob blob) throws RepositoryException {
        return null != blob ? new ValueImpl(BinaryPropertyState.binaryProperty("", blob), namePathMapper, blobAccessProvider) : null;
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

    /**
     * A {@link BlobAccessProvider} implementation that does not support direct
     * binary up- or download.
     */
    private static class DefaultBlobAccessProvider
            implements BlobAccessProvider {

        @Nullable
        @Override
        public BlobUpload initiateBlobUpload(long maxUploadSizeInBytes,
                                             int maxNumberOfURIs) {
            return null;
        }

        @Nullable
        @Override
        public Blob completeBlobUpload(@NotNull String uploadToken) {
            return null;
        }

        @Nullable
        @Override
        public URI getDownloadURI(@NotNull Blob blob,
                                  @NotNull BlobDownloadOptions downloadOptions) { return null; }
    }

}
