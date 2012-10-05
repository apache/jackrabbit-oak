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
package org.apache.jackrabbit.oak.api;

import java.io.InputStream;

import javax.annotation.Nonnull;

/**
 * Immutable representation of a binary value of finite length.
 */
public interface Blob {

    /**
     * Returns a new stream for this value object. Multiple calls to this
     * methods return equal instances: {@code getNewStream().equals(getNewStream())}.
     * @return a new stream for this value based on an internal conversion.
     */
    @Nonnull
    InputStream getNewStream();

    /**
     * Returns the length of this blob.
     *
     * @return the length of this blob.
     */
    long length();

    /**
     * The SHA-256 digest of the underlying stream
     * @return
     */
    byte[] sha256();
}
