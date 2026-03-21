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

import java.io.Serializable;

/**
 * Opaque data identifier used to identify records in a data store.
 * All identifiers must be serializable and implement the standard
 * object equality and hash code methods.
 */
public class DataIdentifier implements Serializable {

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -9197191401131100016L;

    /**
     * Data identifier.
     */
    private final String identifier;

    /**
     * Creates a data identifier from the given string.
     *
     * @param identifier data identifier
     */
    public DataIdentifier(String identifier) {
        this.identifier  = identifier;
    }

    //-------------------------------------------------------------< Object >

    /**
     * Returns the identifier string.
     *
     * @return identifier string
     */
    public String toString() {
        return identifier;
    }

    /**
     * Checks if the given object is a data identifier and has the same
     * string representation as this one.
     *
     * @param object other object
     * @return <code>true</code> if the given object is the same identifier,
     *         <code>false</code> otherwise
     */
    public boolean equals(Object object) {
        return (object instanceof DataIdentifier)
            && identifier.equals(object.toString());
    }

    /**
     * Returns the hash code of the identifier string.
     *
     * @return hash code
     */
    public int hashCode() {
        return identifier.hashCode();
    }

}
