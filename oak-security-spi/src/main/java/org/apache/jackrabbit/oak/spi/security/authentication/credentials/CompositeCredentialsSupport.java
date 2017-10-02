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
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Composite implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport}
 * interface that handles multiple providers.
 */
public final class CompositeCredentialsSupport implements CredentialsSupport {

    @Nonnull
    private final Supplier<Collection<CredentialsSupport>> credentialSupplier;

    private CompositeCredentialsSupport(@Nonnull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
        this.credentialSupplier = credentialSupplier;
    }

    public static CredentialsSupport newInstance(@Nonnull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
        return new CompositeCredentialsSupport(credentialSupplier);
    }

    @Override
    @Nonnull
    public Set<Class> getCredentialClasses() {
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        if (all.isEmpty()) {
            return ImmutableSet.of();
        } else if (all.size() == 1) {
            return all.iterator().next().getCredentialClasses();
        } else {
            Set<Class> classes = newHashSet();
            for (CredentialsSupport c : all) {
                classes.addAll(c.getCredentialClasses());
            }
            return classes;
        }
    }

    @Override
    @CheckForNull
    public String getUserId(@Nonnull Credentials credentials) {
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
    @Nonnull
    public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        if (all.isEmpty()) {
            return ImmutableMap.of();
        } else if (all.size() == 1) {
            return all.iterator().next().getAttributes(credentials);
        } else {
            Map<String, Object> attrs = newHashMap();
            for (CredentialsSupport c : all) {
                attrs.putAll(c.getAttributes(credentials));
            }
            return attrs;
        }
    }

    @Override
    public boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes) {
        boolean set = false;
        Collection<CredentialsSupport> all = this.credentialSupplier.get();
        for (CredentialsSupport c : all) {
            set = c.setAttributes(credentials, attributes) || set;
        }
        return set;
    }
}