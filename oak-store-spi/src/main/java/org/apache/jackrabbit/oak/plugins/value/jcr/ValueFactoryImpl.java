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
import java.util.List;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.plugins.value.ErrorValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ValueFactory} interface.
 */
public class ValueFactoryImpl extends PartialValueFactory implements ValueFactory {

    private static final PerfLogger binOpsLogger = new PerfLogger(
            LoggerFactory.getLogger("org.apache.jackrabbit.oak.jcr.operations.binary.perf"));
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

    /**
     * Utility method for creating a {@code Value} based on a
     * {@code PropertyState}. The {@link Value} instance created by this factory
     * method will not be backed with a {@link BlobAccessProvider} and the
     * {@link Binary} retrieved from the {@link Value} does not provide a
     * download URI, even if the underlying blob store supports it.
     *
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR
     *          names/paths to the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     * @deprecated use {@link PartialValueFactory#createValue(PropertyState)} instead.
     */
    @Deprecated
    public static Value createValue(@NotNull PropertyState property,
                                    @NotNull NamePathMapper namePathMapper) {
        return new PartialValueFactory(namePathMapper).createValue(property);
    }

    /**
     * Utility method for creating a {@code Value} based on a
     * {@code PropertyValue}. The {@link Value} instance created by this factory
     * method will not be backed with a {@link BlobAccessProvider} and the
     * {@link Binary} retrieved from the {@link Value} does not provide a
     * download URI, even if the underlying blob store supports it.
     *
     * Utility method for creating a {@code Value} based on a {@code PropertyValue}.
     * @param property  The property value
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  New {@code Value} instance
     * @throws IllegalArgumentException if {@code property.isArray()} is {@code true}.
     * @deprecated use {@link PartialValueFactory#createValue(PropertyState)} instead.
     */
    @Deprecated
    @NotNull
    public static Value createValue(@NotNull PropertyValue property,
                                    @NotNull NamePathMapper namePathMapper) {
        PropertyState ps = PropertyValues.create(property);
        if (ps == null) {
            throw new IllegalArgumentException("Failed to convert the specified property value to a property state.");
        }
        return new PartialValueFactory(namePathMapper).createValue(ps);
    }

    /**
     * Utility method for creating {@code Value}s based on a
     * {@code PropertyState}. The {@link Value} instances created by this factory
     * method will not be backed with a {@link BlobAccessProvider} and the
     * {@link Binary} retrieved from the {@link Value} does not provide a
     * download URI, even if the underlying blob store supports it.
     *
     * @param property  The property state
     * @param namePathMapper The name/path mapping used for converting JCR names/paths to
     * the internal representation.
     * @return  A list of new {@code Value} instances
     * @deprecated use {@link PartialValueFactory#createValues(PropertyState)} instead.
     */
    @Deprecated
    public static List<Value> createValues(PropertyState property, NamePathMapper namePathMapper) {
        return new PartialValueFactory(namePathMapper).createValues(property);
    }

    //-------------------------------------------------------< ValueFactory >---

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
    public Binary createBinary(InputStream stream) throws RepositoryException {
        try {
            return new BinaryImpl(createBinaryValue(stream));
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

    private ValueImpl createBinaryValue(InputStream value) throws IOException, RepositoryException {
        long start = binOpsLogger.start();
        Blob blob = root.createBlob(value);
        binOpsLogger.end(start, -1, "Created binary property of size [{}]", blob.length());
        return createBinaryValue(blob);
    }

    private ValueImpl createBinaryValue(Blob blob) throws RepositoryException {
        return new ValueImpl(BinaryPropertyState.binaryProperty("", blob), namePathMapper, getBlobAccessProvider());
    }
}
