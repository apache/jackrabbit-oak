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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import com.google.common.collect.ImmutableSet;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(metatype = true, policy = org.apache.felix.scr.annotations.ConfigurationPolicy.REQUIRE)
@Service({AuthorizationConfiguration.class, org.apache.jackrabbit.oak.spi.security.SecurityConfiguration.class})
@Properties({
        @Property(name = CompositeConfiguration.PARAM_RANKING,
                label = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.",
                intValue = 400),
        @Property(name = OAK_SECURITY_NAME,
                propertyPrivate = true,
                value = "org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedAuthorizationConfiguration")
})
public final class PredefinedAuthorizationConfiguration extends ConfigurationBase implements AuthorizationConfiguration {

    private static final long READ_PERMISSIONS = Permissions.READ | Permissions.READ_ACCESS_CONTROL;
    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES);

    @NotNull
    @Override
    public AccessControlManager getAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new AbstractAccessControlManager(root, namePathMapper, getSecurityProvider()) {

            @Override
            public AccessControlPolicy[] getPolicies(String absPath) {
                return new AccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicy[] getEffectivePolicies(String absPath) {
                return new AccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
                return new AccessControlPolicyIteratorAdapter(Collections.emptyIterator());
            }

            @Override
            public void setPolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
                throw new AccessControlException();
            }

            @Override
            public void removePolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
                throw new AccessControlException();
            }

            @Override
            public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) {
                return new JackrabbitAccessControlPolicy[0];
            }

            @Override
            public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) {
                return new JackrabbitAccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicy[] getEffectivePolicies(Set<Principal> set) {
                return new AccessControlPolicy[0];
            }
        };
    }

    @NotNull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return RestrictionProvider.EMPTY;
    }

    @NotNull
    @Override
    public PermissionProvider getPermissionProvider(@NotNull Root root, @NotNull String workspaceName, @NotNull Set<Principal> principals) {
        return new PredefinedPermissionProvider(principals);
    }

    @NotNull
    @Override
    public String getName() {
        return AuthorizationConfiguration.NAME;
    }
}
