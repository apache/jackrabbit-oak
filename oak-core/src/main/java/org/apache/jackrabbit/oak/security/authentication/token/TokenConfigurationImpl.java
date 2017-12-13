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

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CompositeCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.SimpleCredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

/**
 * Default implementation for the {@code TokenConfiguration} interface.
 */
@Component(
        service = {TokenConfiguration.class, SecurityConfiguration.class},
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl")
@Designate(ocd = TokenConfigurationImpl.Configuration.class)
public class TokenConfigurationImpl extends ConfigurationBase implements TokenConfiguration {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak TokenConfiguration"
    )
    @interface Configuration {
        @AttributeDefinition(
                 name = "Token Expiration",
                 description = "Expiration time of login tokens in ms.")
        String tokenExpiration();

        @AttributeDefinition(
                name = "Token Length",
                description = "Length of the generated token.")
        String tokenLength();
        
        @AttributeDefinition(
                name = "Token Refresh",
                description = "Enable/disable refresh of login tokens (i.e. resetting the expiration time).")
        boolean tokenRefresh() default true;

        @AttributeDefinition(
                name = "Token Cleanup Threshold",
                description = "Setting this option to a value > 0 will trigger a cleanup upon token creation: " +
                        "if the number of existing token matches/exceeds the " +
                        "configured value an attempt is made to removed expired tokens.")
        long tokenCleanupThreshold() default TokenProviderImpl.NO_TOKEN_CLEANUP;

        @AttributeDefinition(
                name = "Hash Algorithm",
                description = "Name of the algorithm to hash the token.")
        String passwordHashAlgorithm() default PasswordUtil.DEFAULT_ALGORITHM;

        @AttributeDefinition(
                name = "Hash Iterations",
                description = "Number of iterations used to hash the token.")
        int passwordHashIterations() default PasswordUtil.DEFAULT_ITERATIONS;

        @AttributeDefinition(
                name = "Hash Salt Size",
                description = "Size of the salt used to generate the hash.")
        int passwordSaltSize() default PasswordUtil.DEFAULT_SALT_SIZE;
    }

    private final Map<String, CredentialsSupport> credentialsSupport = new ConcurrentHashMap<>(
            ImmutableMap.of(SimpleCredentialsSupport.class.getName(), SimpleCredentialsSupport.getInstance()));

    @SuppressWarnings("UnusedDeclaration")
    public TokenConfigurationImpl() {
        super();
    }

    public TokenConfigurationImpl(@Nonnull SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //----------------------------------------------------------------< SCR >---
    @SuppressWarnings("UnusedDeclaration")
    @Activate
    // reference to @Configuration class needed for correct DS xml generation
    private void activate(Configuration configuration, Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    @Reference(name = "credentialsSupport",
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC)
    @SuppressWarnings("UnusedDeclaration")
    public void bindCredentialsSupport(CredentialsSupport credentialsSupport) {
        this.credentialsSupport.put(credentialsSupport.getClass().getName(), credentialsSupport);
    }

    @SuppressWarnings("UnusedDeclaration")
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
        ValidatorProvider vp = new TokenValidatorProvider(getSecurityProvider().getParameters(UserConfiguration.NAME), getTreeProvider());
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
        int size = credentialsSupport.size();
        if (size == 0) {
            return SimpleCredentialsSupport.getInstance();
        } else if (size == 1) {
            return credentialsSupport.values().iterator().next();
        } else {
            return CompositeCredentialsSupport.newInstance(() -> ImmutableSet.copyOf(credentialsSupport.values()));
        }
    }
}