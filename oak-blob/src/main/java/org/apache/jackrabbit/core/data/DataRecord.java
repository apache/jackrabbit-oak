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
package org.apache.jackrabbit.core.data;

import java.io.InputStream;

/**
 * Immutable data record that consists of a binary stream.
 */
public interface DataRecord {

    /**
     * Returns the identifier of this record.
     *
     * @return data identifier
     */
    DataIdentifier getIdentifier();

    /**
     * Returns a secure reference to this binary, or {@code null} if no such
     * reference is available.
     *
     * @return binary reference, or {@code null}
     */
    String getReference();

    /**
     * Returns the length of the binary stream in this record.
     *
     * @return length of the binary stream
     * @throws DataStoreException if the record could not be accessed
     */
    long getLength() throws DataStoreException;

    /**
     * Returns the the binary stream in this record.
     *
     * @return binary stream
     * @throws DataStoreException if the record could not be accessed
     */
    InputStream getStream() throws DataStoreException;

    /**
     * Returns the last modified of the record.
     * 
     * @return last modified time of the binary stream
     */
    long getLastModified();
}
