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
package org.apache.jackrabbit.api.security.authentication.token;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.Credentials;
import java.util.HashMap;

/**
 * <code>TokenCredentials</code> implements the <code>Credentials</code>
 * interface and represents single token credentials. Similar to
 * {@link javax.jcr.SimpleCredentials} this credentials implementation allows
 * to set additional attributes.
 */
public final class TokenCredentials implements Credentials {

    private final String token;
    private final HashMap<String, String> attributes = new HashMap<>();

    /**
     * Create a new instance.
     *
     * @param token A token string used to create this credentials instance.
     * @throws IllegalArgumentException If the specified token is <code>null</code>
     * or empty string.
     */
    public TokenCredentials(@NotNull String token) throws IllegalArgumentException {
        if (token == null || token.length() == 0) {
            throw new IllegalArgumentException("Invalid token '" + token + "'");
        }
        this.token = token;
    }

    /**
     * Returns the token this credentials are built from.
     * 
     * @return the token.
     */
    @NotNull
    public String getToken() {
        return token;
    }

    /**
     * Stores an attribute in this credentials instance.
     *
     * @param name  a <code>String</code> specifying the name of the attribute
     * @param value the <code>Object</code> to be stored
     */
    public void setAttribute(@NotNull String name, @Nullable String value) {
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
     * @param name a <code>String</code> specifying the name of the attribute
     * @return an <code>Object</code> containing the value of the attribute, or
     *         <code>null</code> if the attribute does not exist
     */
    @Nullable
    public String getAttribute(@NotNull String name) {
        synchronized (attributes) {
            return (attributes.get(name));
        }
    }

    /**
     * Removes an attribute from this credentials instance.
     *
     * @param name a <code>String</code> specifying the name of the attribute to
     *             remove
     */
    public void removeAttribute(@NotNull String name) {
        synchronized (attributes) {
            attributes.remove(name);
        }
    }

    /**
     * Returns the names of the attributes available to this credentials
     * instance. This method returns an empty array if the credentials instance
     * has no attributes available to it.
     *
     * @return a string array containing the names of the stored attributes
     */
    @NotNull
    public String[] getAttributeNames() {
        synchronized (attributes) {
            return attributes.keySet().toArray(new String[0]);
        }
    }
}