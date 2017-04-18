/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

public class BinaryPropertyState extends SinglePropertyState<Blob> {
    private final Blob value;

    public BinaryPropertyState(@Nonnull String name, @Nonnull Blob value) {
        super(name);
        this.value = checkNotNull(value);
    }

    /**
     * Create a {@code PropertyState} from an array of bytes.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#BINARY}
     */
    public static PropertyState binaryProperty(
            @Nonnull String name, @Nonnull byte[] value) {
        return new BinaryPropertyState(
                name, new ArrayBasedBlob(checkNotNull(value)));
    }

    /**
     * Create a {@code PropertyState} from an array of bytes.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#BINARY}
     */
    public static PropertyState binaryProperty(
            @Nonnull String name, @Nonnull String value) {
        return new BinaryPropertyState(
                name, new StringBasedBlob(checkNotNull(value)));
    }

    /**
     * Create a {@code PropertyState} from a {@link Blob}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#BINARY}
     */
    public static PropertyState binaryProperty(
            @Nonnull String name, @Nonnull Blob value) {
        return new BinaryPropertyState(name, value);
    }

    /**
     * Create a {@code PropertyState} from a {@link javax.jcr.Value}.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#BINARY}
     */
    public static PropertyState binaryProperty(
            @Nonnull String name, @Nonnull Value value) throws RepositoryException {
        return new BinaryPropertyState(name, getBlob(value));
    }

    @Override
    public Blob getValue() {
        return value;
    }

    @Override
    public Converter getConverter() {
        return Conversions.convert(value);
    }

    @Override
    public long size() {
        return value.length();
    }

    @Override
    public Type<?> getType() {
        return Type.BINARY;
    }
}
