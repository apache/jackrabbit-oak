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
package org.apache.jackrabbit.oak.namepath;

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.osgi.annotation.versioning.ProviderType;

/**
 * TODO document
 */
@ProviderType
public interface NameMapper {

    /**
     * Returns the Oak name for the given JCR name, or {@code null} if no
     * such mapping exists because the given JCR name contains an unknown
     * namespace URI or prefix, or is otherwise invalid.
     *
     * @param jcrName JCR name
     * @return Oak name, or {@code null}
     */
    @CheckForNull
    String getOakNameOrNull(@Nonnull String jcrName);

    /**
     * Returns the Oak name for the specified JCR name. In contrast to
     * {@link #getOakNameOrNull(String)} this method will throw a {@code RepositoryException}
     * if the JCR name is invalid and cannot be resolved.
     *
     * @param jcrName The JCR name to be converted.
     * @return A valid Oak name.
     * @throws RepositoryException If the JCR name cannot be resolved.
     */
    @Nonnull
    String getOakName(@Nonnull String jcrName) throws RepositoryException;

    /**
     * Returns the local namespace prefix mappings, or an empty map if
     * there aren't any local mappings.
     *
     * @return local namespace prefix to URI mappings
     */
    @Nonnull
    Map<String, String> getSessionLocalMappings();

    /**
     * Returns the JCR name for the given Oak name. The given name is
     * expected to have come from a valid Oak repository that contains
     * only valid names with proper namespace mappings. If that's not
     * the case, either a programming error or a repository corruption
     * has occurred and an appropriate unchecked exception gets thrown.
     *
     * @param oakName Oak name
     * @return JCR name
     */
    @Nonnull
    String getJcrName(@Nonnull String oakName);

}
