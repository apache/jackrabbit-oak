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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Immutable representation of a binary value of finite length.
 * <p>
 * Two blobs are considered equal in terms of {@link Object#equals(Object)}
 * if they contain the same sequences of bytes. Implementations can optimize
 * the equality checks by using strong hash codes or other similar means as
 * long as they comply with the above definition of equality.
 * <p>
 * Due to their nature blobs should not be used as keys in hash tables.
 * To highlight that and to ensure semantic correctness of the equality
 * contract across different blob implementations, the {@link Object#hashCode()}
 * method of all blob instances should return zero.
 */
public interface Blob {

    /**
     * Returns a new stream for this blob. The streams returned from
     * multiple calls to this method are byte wise equals. That is,
     * subsequent calls to {@link java.io.InputStream#read() read}
     * return the same sequence of bytes as long as neither call throws
     * an exception.
     *
     * @return a new stream for this blob
     */
    @Nonnull
    InputStream getNewStream();

    /**
     * Returns the length of this blob or -1 if unknown.
     *
     * @return the length of this blob.
     */
    long length();

    /**
     * Returns a secure reference to this blob, or {@code null} if such
     * a reference is not available.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-834">OAK-834</a>
     * @return binary reference, or {@code null}
     */
    @CheckForNull
    String getReference();

}
