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
package org.apache.jackrabbit.oak.spi.security.authentication.credentials;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;

public abstract class AbstractCredentials implements Credentials {

    protected final Map<String,Object> attributes = new HashMap();
    protected final String userId;

    public AbstractCredentials(@Nonnull String userId) {
        this.userId = userId;
    }

    /**
     * Returns the userId.
     *
     * @return the userId.
     */
    @Nonnull
    public String getUserId() {
        return userId;
    }

    /**
     * Stores an attribute in this credentials instance. If the specified
     * {@code value} is {@code null} the attribute will be removed.
     *
     * @param name
     *            a {@code String} specifying the name of the attribute
     * @param value
     *            the {@code Object} to be stored
     */
    public void setAttribute(@Nonnull String name, @Nullable Object value) {
        // name cannot be null
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
        }

        // null value is the same as removeAttribute()
        if (value == null) {
            removeAttribute(name);
            return;
        }

        synchronized (attributes) {
            attributes.put(name, value);
        }
    }
    
    /**
     * Returns the value of the named attribute as an <code>Object</code>, or
     * <code>null</code> if no attribute of the given name exists.
     *
     * @param name
     *            a <code>String</code> specifying the name of the attribute
     * @return an <code>Object</code> containing the value of the attribute, or
     *         <code>null</code> if the attribute does not exist
     */
    @CheckForNull
    public Object getAttribute(@Nonnull String name) {
        synchronized (attributes) {
            return (attributes.get(name));
        }
    }

    /**
     * Removes an attribute from this credentials instance.
     *
     * @param name
     *            a <code>String</code> specifying the name of the attribute to
     *            remove
     */
    public void removeAttribute(@Nonnull String name) {
        synchronized (attributes) {
            attributes.remove(name);
        }
    }
 
    /**
     * @return an immutable map containing the attributes available to this credentials instance
     */
    @Nonnull
    public Map<String,Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Stores the attributes in this credentials instance.
     *
     * @param attributes The attributes to be stored
     */
    public void setAttributes(@Nonnull Map<String,Object> attributes) {
        synchronized (attributes) {
            this.attributes.putAll(attributes);
        }
    }
}
