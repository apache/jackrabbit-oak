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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractGroupAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(
        service = AuthorizableActionProvider.class,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalAuthorizableActionProvider")
@Designate(ocd = ExternalAuthorizableActionProvider.Configuration.class)
public final class ExternalAuthorizableActionProvider implements AuthorizableActionProvider {
    
    @ObjectClassDefinition(name = "Apache Jackrabbit Oak ExternalAuthorizableActionProvider", description = "Implementation of the AuthorizableActionProvider that specifically covers operations for users/groups defined by an external IDP that get synced into the repository.")
    @interface Configuration {
        @AttributeDefinition(
                name = "Behavior when Adding Members from Different IDP",
                description = "Defines if Group.addMember should fail when adding a member defined by a different IDP. If set to false only a warning is logged.")
        boolean failAddMembersForDifferentIdp() default false;
    }
    
    private static final Logger log = LoggerFactory.getLogger(ExternalAuthorizableActionProvider.class);
    
    private boolean failAddMembersForDifferentIdp = false;
    
    private SyncHandlerMappingTracker syncHandlerMappingTracker;
    private AutomembershipTracker automembershipTracker;
    
    @Activate
    public void activate(@NotNull BundleContext bundleContext, @NotNull Configuration configuration) {
        failAddMembersForDifferentIdp = configuration.failAddMembersForDifferentIdp();

        syncHandlerMappingTracker = new SyncHandlerMappingTracker(bundleContext);
        syncHandlerMappingTracker.open();

        automembershipTracker = new AutomembershipTracker(bundleContext, syncHandlerMappingTracker);
        automembershipTracker.open();
    }

    @Deactivate
    public void deactivate() {
        if (automembershipTracker != null) {
            automembershipTracker.close();
        }
        if (syncHandlerMappingTracker != null) {
            syncHandlerMappingTracker.close();
        }
    }
    
    @Override
    public @NotNull List<? extends AuthorizableAction> getAuthorizableActions(@NotNull SecurityProvider securityProvider) {
        return Collections.singletonList(new IdpBoundaryAction());
    }

    @Nullable
    private static String getIdpName(@Nullable ExternalIdentityRef ref) {
        return (ref == null) ? null : ref.getProviderName();
    }
    
    private final class IdpBoundaryAction extends AbstractGroupAction {

        @Override
        public void onMemberAdded(@NotNull Group group, @NotNull Authorizable member, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            ExternalIdentityRef memberRef = DefaultSyncContext.getIdentityRef(member);
            String idpName = getIdpName(memberRef);
            if (idpName == null) {
                // member is not an external members identity -> ignore
                return;
            }

            String groupIdpName = getIdpName(DefaultSyncContext.getIdentityRef(group));
            if (idpName.equals(groupIdpName)) {
                // same IDP -> nothing to verify
                return;
            }

            // not the same IDP -> validate
            String groupId = group.getID();
            if (automembershipTracker != null && automembershipTracker.isAutoMembership(idpName, groupId, member.isGroup())) {
                // ignore groups that are configured in 'automembership' option for the given IDP
                return;
            }

            String extId = memberRef.getString();
            String msg = String.format("Attempt to add external identity '%s' as member of group '%s' defined by IDP '%s'.", extId, groupId, groupIdpName);
            log.warn(msg);
            if (failAddMembersForDifferentIdp) {
                throw new ConstraintViolationException(msg);
            }
        }
    }

    private static final class AutomembershipTracker extends ServiceTracker {

        private final SyncHandlerMappingTracker mappingTracker;
        private final Set<ServiceReference> refs = new HashSet<>();

        private final Map<String, Set<String>> userAutoMembership = new HashMap<>();
        private final Map<String, Set<String>> groupAutoMembership = new HashMap<>();

        AutomembershipTracker(@NotNull BundleContext context, @NotNull SyncHandlerMappingTracker mappingTracker) {
            super(context, SyncHandler.class.getName(), null);
            this.mappingTracker = mappingTracker;
        }

        @Override
        public Object addingService(ServiceReference reference) {
            refs.add(reference);
            userAutoMembership.clear(); // recalculate
            groupAutoMembership.clear(); // recalculate
            return super.addingService(reference);
        }

        @Override
        public void modifiedService(ServiceReference reference, Object service) {
            refs.add(reference);
            userAutoMembership.clear(); // recalculate
            groupAutoMembership.clear(); // recalculate            
            super.modifiedService(reference, service);
        }

        @Override
        public void removedService(ServiceReference reference, Object service) {
            refs.remove(reference);
            userAutoMembership.clear(); // recalculate
            groupAutoMembership.clear(); // recalculate
            super.removedService(reference, service);
        }
        
        @NotNull
        private Map<String, Set<String>> getAutomembership(boolean memberIsGroup) {
            Map<String, Set<String>> autoMembership = (memberIsGroup) ? groupAutoMembership : userAutoMembership;
            if (autoMembership.isEmpty() && !refs.isEmpty()) {
                for (ServiceReference ref : refs) {
                    String syncHandlerName = PropertiesUtil.toString(ref.getProperty(DefaultSyncConfigImpl.PARAM_NAME), DefaultSyncConfigImpl.PARAM_NAME_DEFAULT);
                    String[] userMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP), new String[0]);
                    String[] groupMembership = PropertiesUtil.toStringArray(ref.getProperty(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP), new String[0]);

                    for (String idpName : mappingTracker.getIdpNames(syncHandlerName)) {
                        updateAutoMembershipMap(userAutoMembership, syncHandlerName, idpName, userMembership);
                        updateAutoMembershipMap(groupAutoMembership, syncHandlerName, idpName, groupMembership);
                    }
                }
            }
            return autoMembership;
        }
        
        private static void updateAutoMembershipMap(@NotNull Map<String, Set<String>> map, @NotNull String syncHandlerName, 
                                                    @NotNull String idpName, @NotNull String[] membership) {
            Set<String> userMembership = CollectionUtils.toSet(membership);
            Set<String> previous = map.put(idpName, userMembership);
            if (previous != null) {
                String msg = previous.equals(userMembership) ? "Duplicate" : "Colliding";
                log.debug("{} auto-membership configuration for IDP '{}'; replacing previous values {} by {} defined by SyncHandler '{}'", msg, idpName, previous, userMembership, syncHandlerName);
            }
        }
        
        private boolean isAutoMembership(@NotNull String idpName, @NotNull String groupId, boolean memberIsGroup) {
            Set<String> ids = getAutomembership(memberIsGroup).get(idpName);
            return ids != null && ids.contains(groupId);
        }
    }
}