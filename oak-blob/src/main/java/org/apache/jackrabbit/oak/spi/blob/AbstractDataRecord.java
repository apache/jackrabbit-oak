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
package org.apache.jackrabbit.oak.spi.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;

/**
 * Implements {@link DataRecord}
 */
public abstract class AbstractDataRecord implements DataRecord {

    /**
     * The data store that contains this record.
     */
    protected final AbstractSharedBackend backend;

    /**
     * The binary identifier;
     */
    private final DataIdentifier identifier;

    /**
     * Creates a data record with the given identifier.
     *
     * @param identifier data identifier
     */
    public AbstractDataRecord(
        AbstractSharedBackend backend, DataIdentifier identifier) {
        this.backend = backend;
        this.identifier = identifier;
    }

    /**
     * Returns the data identifier.
     *
     * @return data identifier
     */
    public DataIdentifier getIdentifier() {
        return identifier;
    }

    /**
     * Delegates the call to the backend to retrieve reference.
     *
     * @return
     */
    public String getReference() {
        return backend.getReferenceFromIdentifier(identifier);
    }

    /**
     * Returns the string representation of the data identifier.
     *
     * @return string representation
     */
    public String toString() {
        return identifier.toString();
    }

    /**
     * Checks if the given object is a data record with the same identifier
     * as this one.
     *
     * @param object other object
     * @return <code>true</code> if the other object is a data record and has
     *         the same identifier as this one, <code>false</code> otherwise
     */
    public boolean equals(Object object) {
        return (object instanceof DataRecord)
            && identifier.equals(((DataRecord) object).getIdentifier());
    }

    /**
     * Returns the hash code of the data identifier.
     *
     * @return hash code
     */
    public int hashCode() {
        return identifier.hashCode();
    }
}
