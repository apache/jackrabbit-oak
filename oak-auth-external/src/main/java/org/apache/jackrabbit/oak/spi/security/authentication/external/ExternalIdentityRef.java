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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.util.Text;

/**
 * {@code ExternalIdentityRef} defines a reference to an external identity.
 */
public class ExternalIdentityRef {

    private final String id;

    private final String providerName;

    private final String string;

    /**
     * Creates a new external identity ref with the given id and provider name
     * @param id the id of the identity.
     * @param providerName the name of the identity provider
     */
    public ExternalIdentityRef(@Nonnull String id, @CheckForNull String providerName) {
        this.id = id;
        this.providerName = providerName;

        StringBuilder b = new StringBuilder();
        b.append(Text.escape(id));
        if (providerName != null && providerName.length() > 0) {
            b.append('@').append(Text.escape(providerName));
        }
        string =  b.toString();
    }

    /**
     * Returns the name of the identity provider.
     * @return the name of the identity provider.
     */
    @CheckForNull
    public String getProviderName() {
        return providerName;
    }

    /**
     * Returns the id of the external identity. for example the DN of an LDAP user.
     * @return the id
     */
    @Nonnull
    public String getId() {
        return id;
    }

    /**
     * Returns a string representation of this external identity reference
     * @return a string representation.
     */
    @Nonnull
    public String getString() {
        return string;
    }

    /**
     * Creates an external identity reference from a string representation.
     * @param str the string
     * @return the reference
     */
    public static ExternalIdentityRef fromString(@Nonnull String str) {
        int idx = str.indexOf('@');
        if (idx < 0) {
            return new ExternalIdentityRef(Text.unescape(str), null);
        } else {
            return new ExternalIdentityRef(
                    Text.unescape(str.substring(0, idx)),
                    Text.unescape(str.substring(idx+1))
            );
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalIdentityRef{");
        sb.append("id='").append(id).append('\'');
        sb.append(", providerName='").append(providerName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Tests if the given object is an external identity reference and if it's getString() is equal to this.
     */
    @Override
    public boolean equals(Object o) {
        try {
            // assuming that we never compare other types of classes
            return this == o || string.equals(((ExternalIdentityRef) o).string);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * @return same as {@code this.getString().hashCode()}
     */
    @Override
    public int hashCode() {
        return string.hashCode();
    }
}