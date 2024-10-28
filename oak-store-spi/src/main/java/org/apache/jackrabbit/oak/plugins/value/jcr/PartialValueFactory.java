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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.value.jcr.ValueImpl.newValue;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.api.blob.BlobUploadOptions;
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
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A partial value factory implementation that only deals with in-memory values
 * and can wrap a {@link Value} around a {@link PropertyState}.
 */
public class PartialValueFactory {

    /**
     * This default blob access provider is a no-op implementation.
     */
    @NotNull
    public static final BlobAccessProvider DEFAULT_BLOB_ACCESS_PROVIDER = new DefaultBlobAccessProvider();

    @NotNull
    private final NamePathMapper namePathMapper;

    @NotNull
    private final BlobAccessProvider blobAccessProvider;

    /**
     * Creates a new value factory stub using the given {@link NamePathMapper}.
     * The factory instance created with this constructor does not have a
     * {@link BlobAccessProvider} and any {@link Binary} retrieved from a
     * {@link Value} returned by this factory instance will not provide a
     * download URI.
     *
     * @param namePathMapper the name path mapper.
     */
    public PartialValueFactory(@NotNull NamePathMapper namePathMapper) {
        this(namePathMapper, DEFAULT_BLOB_ACCESS_PROVIDER);
    }

    /**
     * Creates a new value factory stub using the given {@link NamePathMapper}
     * and {@link BlobAccessProvider}.
     *
     * @param namePathMapper the name path mapper.
     * @param blobAccessProvider the blob access provider.
     */
    public PartialValueFactory(@NotNull NamePathMapper namePathMapper,
                               @NotNull BlobAccessProvider blobAccessProvider) {
        this.namePathMapper = requireNonNull(namePathMapper);
        this.blobAccessProvider = requireNonNull(blobAccessProvider);
    }

    @NotNull
    BlobAccessProvider getBlobAccessProvider() {
        return blobAccessProvider;
    }

    /**
     * @return the {@link NamePathMapper} used by this value factory.
     */
    @NotNull
    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    /**
     * Utility method for creating a {@code Value} based on a
     * {@code PropertyState}.
     *
     * @param property The property state
     * @return New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is
     *         {@code true}.
     */
    @NotNull
    public Value createValue(@NotNull PropertyState property) {
        return newValue(property, namePathMapper, blobAccessProvider);
    }

    /**
     * Utility method for creating {@code Value}s based on a
     * {@code PropertyState}.
     *
     * @param property The property state
     * @return A list of new {@code Value} instances
     */
    @NotNull
    public List<Value> createValues(@NotNull PropertyState property) {
        List<Value> values = new ArrayList<>();
        for (int i = 0; i < property.count(); i++) {
            values.add(newValue(property, i, namePathMapper, getBlobAccessProvider()));
        }
        return values;
    }

    //-------------------------------------------------------< ValueFactory >---

    @NotNull
    public Value createValue(@NotNull String value) {
        return newValue(StringPropertyState.stringProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(long value) {
        return newValue(LongPropertyState.createLongProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(double value) {
        return newValue(DoublePropertyState.doubleProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(@NotNull Calendar value) {
        return newValue(PropertyStates.createProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(boolean value) {
        return newValue(BooleanPropertyState.booleanProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(@NotNull Node value) throws RepositoryException {
        return createValue(value, false);
    }

    @NotNull
    public Value createValue(@NotNull Node value, boolean weak) throws RepositoryException {
        if (!value.isNodeType(NodeType.MIX_REFERENCEABLE)) {
            throw new ValueFormatException(
                    "Node is not referenceable: " + value.getPath());
        }
        return weak
                ? newValue(GenericPropertyState.weakreferenceProperty("", value.getUUID()), namePathMapper, getBlobAccessProvider())
                : newValue(GenericPropertyState.referenceProperty("", value.getUUID()), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(@NotNull BigDecimal value) {
        return newValue(DecimalPropertyState.decimalProperty("", value), namePathMapper, getBlobAccessProvider());
    }

    @NotNull
    public Value createValue(String value, int type) throws ValueFormatException {
        if (value == null) {
            throw new ValueFormatException("null");
        }

        try {
            switch (type) {
                case PropertyType.STRING:
                    return createValue(value);
                case PropertyType.BINARY:
                    return newValue(BinaryPropertyState.binaryProperty("", value), namePathMapper, getBlobAccessProvider());
                case PropertyType.LONG:
                    return createValue(Conversions.convert(value).toLong());
                case PropertyType.DOUBLE:
                    return createValue(Conversions.convert(value).toDouble());
                case PropertyType.DATE:
                    if (ISO8601.parse(value) == null) {
                        throw new ValueFormatException("Invalid date " + value);
                    }
                    return newValue(GenericPropertyState.dateProperty("", value), namePathMapper, getBlobAccessProvider());
                case PropertyType.BOOLEAN:
                    return createValue(Conversions.convert(value).toBoolean());
                case PropertyType.NAME:
                    String oakName = namePathMapper.getOakNameOrNull(value);
                    if (oakName == null || !JcrNameParser.validate(oakName)) {
                        throw new ValueFormatException("Invalid name: " + value);
                    }
                    return newValue(GenericPropertyState.nameProperty("", oakName), namePathMapper, getBlobAccessProvider());
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
                    return newValue(GenericPropertyState.pathProperty("", oakValue), namePathMapper, getBlobAccessProvider());
                case PropertyType.REFERENCE:
                    if (!UUIDUtils.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid reference value " + value);
                    }
                    return newValue(GenericPropertyState.referenceProperty("", value), namePathMapper, getBlobAccessProvider());
                case PropertyType.WEAKREFERENCE:
                    if (!UUIDUtils.isValidUUID(value)) {
                        throw new ValueFormatException("Invalid weak reference value " + value);
                    }
                    return newValue(GenericPropertyState.weakreferenceProperty("", value), namePathMapper, getBlobAccessProvider());
                case PropertyType.URI:
                    new URI(value);
                    return newValue(GenericPropertyState.uriProperty("", value), namePathMapper, getBlobAccessProvider());
                case PropertyType.DECIMAL:
                    return createValue(Conversions.convert(value).toDecimal());
                default:
                    throw new ValueFormatException("Invalid type: " + type);
            }
        } catch (NumberFormatException | URISyntaxException e) {
            throw new ValueFormatException("Invalid value " + value + " for type " + PropertyType.nameFromValue(type), e);
        }
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
            return initiateBlobUpload(maxUploadSizeInBytes, maxNumberOfURIs, BlobUploadOptions.DEFAULT);
        }

        @Nullable
        public BlobUpload initiateBlobUpload(long maxUploadSizeInBytes,
                                             int maxNumberOfURIs,
                                             @NotNull final BlobUploadOptions options) {
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
