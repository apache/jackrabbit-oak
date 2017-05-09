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
        this.providerName = (providerName == null || providerName.isEmpty()) ? null : providerName;

        StringBuilder b = new StringBuilder();
        escape(b, id);
        if (this.providerName != null) {
            b.append(';');
            escape(b, this.providerName);
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
    @Nonnull
    public static ExternalIdentityRef fromString(@Nonnull String str) {
        int idx = str.indexOf(';');
        if (idx < 0) {
            return new ExternalIdentityRef(Text.unescape(str), null);
        } else {
            return new ExternalIdentityRef(
                    Text.unescape(str.substring(0, idx)),
                    Text.unescape(str.substring(idx+1))
            );
        }
    }

    /**
     * Escapes the given string and appends it to the builder.
     * @param builder the builder
     * @param str the string
     */
    private static void escape(@Nonnull StringBuilder builder, @Nonnull CharSequence str) {
        final int len = str.length();
        for (int i=0; i<len; i++) {
            char c = str.charAt(i);
            if (c == '%') {
                builder.append("%25");
            } else if (c == ';') {
                builder.append("%3b");
            } else {
                builder.append(c);
            }
        }
    }

    @Override
    public String toString() {
        return "ExternalIdentityRef{" + "id='" + id + '\'' + ", providerName='" + providerName + '\'' + '}';
    }

    /**
     * Tests if the given object is an external identity reference and if it's
     * getString() is equal to this. Note, that there is no need to
     * include {@code id} and {@code provider} fields in the comparison as
     * the string representation already incorporates both.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ExternalIdentityRef) {
            return string.equals(((ExternalIdentityRef) o).string);
        }
        return false;
    }

    /**
     * @return same as {@code this.getString().hashCode()}
     */
    @Override
    public int hashCode() {
        return string.hashCode();
    }
}