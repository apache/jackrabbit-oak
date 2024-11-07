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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.jcr.Credentials;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Composite implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport}
 * interface that handles multiple providers.
 */
public final class CompositeCredentialsSupport implements CredentialsSupport {

    @NotNull
    private final Supplier<Collection<CredentialsSupport>> credentialSupplier;

    private CompositeCredentialsSupport(@NotNull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
        this.credentialSupplier = credentialSupplier;
    }

    public static CredentialsSupport newInstance(@NotNull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
        return new CompositeCredentialsSupport(credentialSupplier);
    }

    @Override
    @NotNull
    public Set<Class> getCredentialClasses() {
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        if (all.isEmpty()) {
            return Set.of();
        } else if (all.size() == 1) {
            return all.iterator().next().getCredentialClasses();
        } else {
            Set<Class> classes = new HashSet<>();
            for (CredentialsSupport c : all) {
                classes.addAll(c.getCredentialClasses());
            }
            return classes;
        }
    }

    @Override
    @Nullable
    public String getUserId(@NotNull Credentials credentials) {
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        for (CredentialsSupport c : all) {
            String userId = c.getUserId(credentials);
            if (userId != null) {
                return userId;
            }
        }
        return null;
    }

    @Override
    @NotNull
    public Map<String, ?> getAttributes(@NotNull Credentials credentials) {
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        if (all.isEmpty()) {
            return ImmutableMap.of();
        } else if (all.size() == 1) {
            return all.iterator().next().getAttributes(credentials);
        } else {
            Map<String, Object> attrs = new HashMap<>();
            for (CredentialsSupport c : all) {
                attrs.putAll(c.getAttributes(credentials));
            }
            return attrs;
        }
    }

    @Override
    public boolean setAttributes(@NotNull Credentials credentials, @NotNull Map<String, ?> attributes) {
        boolean set = false;
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        for (CredentialsSupport c : all) {
            set = c.setAttributes(credentials, attributes) || set;
        }
        return set;
    }
}
