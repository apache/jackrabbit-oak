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
package org.apache.jackrabbit.oak.security.user;

import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

/**
 * Internal interface covering those methods of {link Authorizable} that deal
 * with reading and writing custom properties defined with user and groups.
 */
interface AuthorizableProperties {

    /**
     * Retrieve all property names located at the given relative path underneath
     * the associated {@code Authorizable} instance.
     *
     * @param relPath A relative path referring to a node associated with a
     * given authorizable. A relative path consisting only of the current
     * element "." refers to the authorizable node itself.
     * @return An iterator of property names available at the given {@code relPath}.
     * @throws RepositoryException If an error occurs.
     */
    @Nonnull
    Iterator<String> getNames(@Nonnull String relPath) throws RepositoryException;

    /**
     * Returns {@code true} if there is a custom authorizable property at the
     * specified {@code relPath}; false if no such property exists or if the
     * path refers to a property located outside of the authorizable tree
     * or a protected property that cannot be read/modified using
     * the {@code AuthorizableProperties}.
     *
     * @param relPath A relative path to a property.
     * @return {@code true} if a valid property exists; {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean hasProperty(@Nonnull String relPath) throws RepositoryException;

    /**
     * Returns the values of the property identified by the specified
     * {@code relPath}. If no such property exists or the property doesn't
     * represent a custom property associated with this authorizable {@code null}
     * is returned.
     *
     * @param relPath A relative path to an authorizable property.
     * @return The value(s) of the specified property or {@code null} if no
     * such property exists or the property doesn't represent a valid authorizable
     * property exposed by this interface.
     * @throws RepositoryException If an error occurs.
     */
    @CheckForNull
    Value[] getProperty(@Nonnull String relPath) throws RepositoryException;

    /**
     * Creates or modifies the property at the specified {@code relPath}. If
     * the property exists and is multi-valued it is converted into a single
     * valued property.
     *
     * @param relPath A relative path referring to a custom authorizable property
     * located underneath the associated authorizable.
     * @param value The value of the property.
     * @throws RepositoryException If the {@code relPath} refers to an invalid
     * property: located outside of the scope of this authorizable or one
     * that represents protected content that cannot be modified using this API.
     * @see #setProperty(String, javax.jcr.Value[])
     */
    void setProperty(@Nonnull String relPath, @Nullable Value value) throws RepositoryException;

    /**
     * Creates or modifies the property at the specified {@code relPath}. If
     * the property exists and is single-valued it is converted into a multi-
     * value property.
     *
     * @param relPath A relative path referring to a custom authorizable property
     * associated with the authorizable.
     * @param values The values of the property.
     * @throws RepositoryException If the {@code relPath} refers to an invalid
     * property: located outside of the scope of this authorizable or one
     * that represents protected content that cannot be modified using this API.
     * @see #setProperty(String, javax.jcr.Value)
     */
    void setProperty(@Nonnull String relPath, @Nullable Value[] values) throws RepositoryException;

    /**
     * Removes the property identified by the given {@code relPath} and returns
     * {@code true} if the property was successfully removed.
     *
     * @param relPath A relative path referring to a custom authorizable property
     * associated with the authorizable.
     * @return {@code true} if the property exists and could successfully be
     * removed; {@code false} if no such property exists.
     * @throws RepositoryException If the specified path points to a property
     * that cannot be altered using this API because it is not associated with
     * this authorizable or is otherwise considered protected.
     */
    boolean removeProperty(@Nonnull String relPath) throws RepositoryException;

}