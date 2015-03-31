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
package org.apache.jackrabbit.oak.api;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * {@code AuthInfo} instances provide access to information related
 * to authentication and authorization of a given content session.
 * {@code AuthInfo} instances are guaranteed to be immutable.
 */
public interface AuthInfo {

    AuthInfo EMPTY = new AuthInfo() {
        @Override
        public String getUserID() {
            return null;
        }

        @Nonnull
        @Override
        public String[] getAttributeNames() {
            return new String[0];
        }

        @Override
        public Object getAttribute(String attributeName) {
            return null;
        }

        @Nonnull
        @Override
        public Set<Principal> getPrincipals() {
            return Collections.emptySet();
        }

        @Override
        public String toString() {
            return "empty";
        }
    };

    /**
     * Return the user ID to be exposed on the JCR Session object. It refers
     * to the ID of the user associated with the Credentials passed to the
     * repository login.
     *
     * @return the user ID such as exposed on the JCR Session object.
     */
    @CheckForNull
    String getUserID();

    /**
     * Returns the attribute names associated with this instance.
     *
     * @return The attribute names with that instance or an empty array if
     * no attributes are present.
     */
    @Nonnull
    String[] getAttributeNames();

    /**
     * Returns the attribute with the given name or {@code null} if no attribute
     * with that {@code attributeName} exists.
     *
     * @param attributeName The attribute name.
     * @return The attribute or {@code null}.
     */
    @CheckForNull
    Object getAttribute(String attributeName);

    /**
     * Returns the set of principals associated with this {@code AuthInfo} instance.
     *
     * @return A set of principals.
     */
    @Nonnull
    Set<Principal> getPrincipals();
}
