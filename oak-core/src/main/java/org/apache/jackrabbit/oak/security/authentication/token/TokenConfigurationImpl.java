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
package org.apache.jackrabbit.oak.security.authentication.token;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;

/**
 * Default implementation for the {@code TokenConfiguration} interface.
 */
@Component(metatype = true, label = "Apache Jackrabbit Oak TokenConfiguration")
@Service({TokenConfiguration.class, SecurityConfiguration.class})
@Properties({
        @Property(name = TokenProvider.PARAM_TOKEN_EXPIRATION,
                label = "Token Expiration",
                description = "Expiration time of login tokens in ms."),
        @Property(name = TokenProvider.PARAM_TOKEN_LENGTH,
                label = "Token Length",
                description = "Length of the generated token."),
        @Property(name = TokenProvider.PARAM_TOKEN_REFRESH,
                label = "Token Refresh",
                description = "Enable/disable refresh of login tokens (i.e. resetting the expiration time).",
                boolValue = true),
        @Property(name = UserConstants.PARAM_PASSWORD_HASH_ALGORITHM,
                label = "Hash Algorithm",
                description = "Name of the algorithm to hash the token.",
                value = PasswordUtil.DEFAULT_ALGORITHM),
        @Property(name = UserConstants.PARAM_PASSWORD_HASH_ITERATIONS,
                label = "Hash Iterations",
                description = "Number of iterations used to hash the token.",
                intValue = PasswordUtil.DEFAULT_ITERATIONS),
        @Property(name = UserConstants.PARAM_PASSWORD_SALT_SIZE,
                label = "Hash Salt Size",
                description = "Size of the salt used to generate the hash.",
                intValue = PasswordUtil.DEFAULT_SALT_SIZE)
})
@References({
    @Reference(
            name = "credentialsSupport",
            referenceInterface = CredentialsSupport.class,
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policy = ReferencePolicy.DYNAMIC)
})
public class TokenConfigurationImpl extends ConfigurationBase implements TokenConfiguration {

    private final Map<String, CredentialsSupport> credentialsSupport = new ConcurrentHashMap<>(
            ImmutableMap.of(SimpleCredentialsSupport.class.getName(), SimpleCredentialsSupport.getInstance()));

    @SuppressWarnings("UnusedDeclaration")
    public TokenConfigurationImpl() {
        super();
    }

    public TokenConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //----------------------------------------------------------------< SCR >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    public void bindCredentialsSupport(CredentialsSupport credentialsSupport) {
        this.credentialsSupport.put(credentialsSupport.getClass().getName(), credentialsSupport);
    }

    public void unbindCredentialsSupport(CredentialsSupport credentialsSupport) {
        this.credentialsSupport.remove(credentialsSupport.getClass().getName());
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
        ValidatorProvider vp = new TokenValidatorProvider(getSecurityProvider().getParameters(UserConfiguration.NAME));
        return ImmutableList.of(vp);
    }

    //-------------------------------------------------< TokenConfiguration >---
    /**
     * Returns a new instance of {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider}.
     *
     * @param root The target root.
     * @return A new instance of {@link org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider}.
     */
    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root) {
        UserConfiguration uc = getSecurityProvider().getConfiguration(UserConfiguration.class);
        return new TokenProviderImpl(root, getParameters(), uc, newCredentialsSupport());
    }

    private CredentialsSupport newCredentialsSupport() {
        if (!credentialsSupport.isEmpty()) {
            return CompositeCredentialsSupport.newInstance(new Supplier<Collection<CredentialsSupport>>() {
                @Override
                public Collection<CredentialsSupport> get() {
                    return ImmutableSet.copyOf(credentialsSupport.values());
                }
            });
        } else {
            return SimpleCredentialsSupport.getInstance();
        }
    }

    /**
     * Composite implementation of the
     * {@link org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport}
     * interface that handles multiple providers.
     */
    private static class CompositeCredentialsSupport implements CredentialsSupport {

        @Nonnull
        private final Supplier<Collection<CredentialsSupport>> credentialSupplier;

        private CompositeCredentialsSupport(@Nonnull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
            this.credentialSupplier = credentialSupplier;
        }

        public static CredentialsSupport newInstance(
                @Nonnull Supplier<Collection<CredentialsSupport>> credentialSupplier) {
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
}