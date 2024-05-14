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
package org.apache.jackrabbit.oak.plugins.memory;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StringPropertyState extends SinglePropertyState<String> {
    private final String value;
    private byte[] compressedValue;
    private Compression compression = Compression.GZIP;

    public StringPropertyState(@NotNull String name, @NotNull String value) {
        super(name);
        checkNotNull(value);
        int size = value.getBytes().length;
        System.out.println("size = " + size);
        if (size > 0) {//todo: introduce a threshold
           compressedValue =  compress(value.getBytes());
            System.out.println("compressedValue = " + compressedValue.length);
           this.value = null;
        } else {
            this.value = value;
        }
    }

    private byte[] compress(byte[] value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            OutputStream compressionOutputStream = compression.getOutputStream(out);
            compressionOutputStream.write(value);
            compressionOutputStream.close();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data", e);
        }
    }

    private String decompress(byte[] value) {
        try {
            return new String(compression.getInputStream(new ByteArrayInputStream(value)).readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data", e);
        }
    }

    /**
     * Create a {@code PropertyState} from a string.
     * @param name  The name of the property state
     * @param value  The value of the property state
     * @return  The new property state of type {@link Type#STRING}
     */
    public static PropertyState stringProperty(
            @NotNull String name, @NotNull String value) {
        return new StringPropertyState(name, value);
    }

    @Override
    public String getValue() {
        return value != null ? value : decompress(this.compressedValue);
    }

    @Override
    public Converter getConverter() {
        return Conversions.convert(getValue());
    }

    @Override
    public Type<?> getType() {
        return STRING;
    }

}
