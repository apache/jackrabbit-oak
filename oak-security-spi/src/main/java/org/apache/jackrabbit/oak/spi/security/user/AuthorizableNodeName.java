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
package org.apache.jackrabbit.oak.spi.security.user;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.util.Text;

/**
 * The {@code AuthorizableNodeName} is in charge of generating a valid node
 * name from a given authorizable ID.
 *
 * @since OAK 1.0
 */
public interface AuthorizableNodeName {

    /**
     * Default {@code AuthorizableNodeName} instance.
     *
     * @see AuthorizableNodeName.Default
     */
    AuthorizableNodeName DEFAULT = new Default();

    /**
     * Generates a node name from the specified {@code authorizableId}.
     *
     * @param authorizableId The ID of the authorizable to be created.
     * @return A valid node name.
     */
    @Nonnull
    String generateNodeName(@Nonnull String authorizableId);

    /**
     * Default implementation of the {@code AuthorizableNodeName} interface
     * that uses the specified authorizable identifier as node name
     * {@link org.apache.jackrabbit.util.Text#escapeIllegalJcrChars(String) escaping}
     * any illegal JCR chars.
     */
    final class Default implements AuthorizableNodeName {

        @Override
        @Nonnull
        public String generateNodeName(@Nonnull String authorizableId) {
            return Text.escapeIllegalJcrChars(authorizableId);
        }
    }
}