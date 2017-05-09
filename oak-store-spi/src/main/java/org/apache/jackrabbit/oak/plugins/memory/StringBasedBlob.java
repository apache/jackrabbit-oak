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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;

/**
 * This {@code Blob} implementations is based on a string.
 */
public class StringBasedBlob extends AbstractBlob {
    private final String value;

    public StringBasedBlob(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    /**
     * This implementation returns the bytes of the UTF-8 encoding
     * of the underlying string.
     */
    @Nonnull
    @Override
    public InputStream getNewStream() {
        return new ByteArrayInputStream(value.getBytes(Charsets.UTF_8));
    }

    /**
     * This implementation returns the number of bytes in the UTF-8 encoding
     * of the underlying string.
     */
    @Override
    public long length() {
        return value.getBytes(Charsets.UTF_8).length;
    }
}
