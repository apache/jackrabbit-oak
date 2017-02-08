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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.PrincipalNameResolver;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncResultImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of the {@code DefaultSyncContext} that doesn't synchronize group
 * membership of new external users into the user management of the repository.
 * Instead it will only synchronize the principal names up to the configured depths.
 * In combination with the a dedicated {@code PrincipalConfiguration} this allows
 * to benefit from the repository's authorization model (which is solely
 * based on principals) i.e. full compatibility with the default approach without
 * the complication of synchronizing user management information into the repository,
 * when user management is effectively take care of by the third party system.
 *
 * With the {@link org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler}
 * this feature can be turned on using
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig.User#setDynamicMembership(boolean)}
 *
 * Note: users and groups that have been synchronized before the dynamic membership
 * feature has been enabled will continue to be synchronized in the default way
 * and this context doesn't take effect.
 *
 * @since Oak 1.5.3
 */
public class DynamicSyncContext extends DefaultSyncContext {

    private static final Logger log = LoggerFactory.getLogger(DynamicSyncContext.class);

    public DynamicSyncContext(@Nonnull DefaultSyncConfig config,
                              @Nonnull ExternalIdentityProvider idp,
                              @Nonnull UserManager userManager,
                              @Nonnull ValueFactory valueFactory) {
        super(config, idp, userManager, valueFactory);
    }

    //--------------------------------------------------------< SyncContext >---
    @Nonnull
    @Override
    public SyncResult sync(@Nonnull ExternalIdentity identity) throws SyncException {
        if (identity instanceof ExternalUser) {
            return super.sync(identity);
        } else if (identity instanceof ExternalGroup) {
            try {
                Group group = getAuthorizable(identity, Group.class);
                if (group != null) {
                    // group has been synchronized before -> continue updating for consistency.
                    return syncGroup((ExternalGroup) identity, group);
                } else {
                    // external group has never been synchronized before:
                    // don't sync external groups into the repository internal user management
                    // but limit synchronized information to group-principals stored
                    // separately with each external user such that the subject gets
                    // properly populated upon login
                    ExternalIdentityRef ref = identity.getExternalId();

                    log.debug("ExternalGroup {}: Not synchronized as authorizable Group into the repository.", ref.getString());

                    SyncResult.Status status = (isSameIDP(ref)) ? SyncResult.Status.NOP : SyncResult.Status.FOREIGN;
                    return new DefaultSyncResultImpl(new DefaultSyncedIdentity(identity.getId(), ref, true, -1), status);
                }
            } catch (RepositoryException e) {
                throw new SyncException(e);
            }
        } else {
            throw new IllegalArgumentException("identity must be user or group but was: " + identity);
        }
    }

    //-------------------------------------------------< DefaultSyncContext >---
    @Override
    protected void syncMembership(@Nonnull ExternalIdentity external, @Nonnull Authorizable auth, long depth) throws RepositoryException {
        if (auth.isGroup()) {
            return;
        }

        if (auth.hasProperty(REP_LAST_SYNCED) && !auth.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES)) {
            // user has been synchronized before dynamic membership has been turned on
            super.syncMembership(external, auth, depth);
        } else {
            // retrieve membership of the given external user (up to the configured
            // depth) and add (or replace) the rep:externalPrincipalNames property
            // with the accurate collection of principal names.
            try {
                Value[] vs;
                if (depth <= 0) {
                    vs = new Value[0];
                } else {
                    Set<String> principalsNames = new HashSet<String>();
                    collectPrincipalNames(principalsNames, external.getDeclaredGroups(), depth);
                    vs = createValues(principalsNames);
                }
                auth.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, vs);
            } catch (ExternalIdentityException e) {
                log.error("Failed to synchronize membership information for external identity " + external.getId(), e);
            }
        }
    }

    @Override
    protected void applyMembership(@Nonnull Authorizable member, @Nonnull Set<String> groups) throws RepositoryException {
        log.debug("Dynamic membership sync enabled => omit setting auto-membership for {} ", member.getID());
    }

    /**
     * Recursively collect the principal names of the given declared group
     * references up to the given depth.
     *
     * @param principalNames The set used to collect the names of the group principals.
     * @param declaredGroupIdRefs The declared group references for a user or a group.
     * @param depth Configured membership nesting; the recursion will be stopped once depths is < 1.
     * @throws ExternalIdentityException If an error occurs while resolving the the external group references.
     */
    private void collectPrincipalNames(@Nonnull Set<String> principalNames, @Nonnull Iterable<ExternalIdentityRef> declaredGroupIdRefs, long depth) throws ExternalIdentityException {
        boolean shortcut = (depth <= 1 && idp instanceof PrincipalNameResolver);
        for (ExternalIdentityRef ref : declaredGroupIdRefs) {
            if (shortcut) {
                principalNames.add(((PrincipalNameResolver) idp).fromExternalIdentityRef(ref));
            } else {
                // get group from the IDP
                ExternalIdentity extId = idp.getIdentity(ref);
                if (extId instanceof ExternalGroup) {
                    principalNames.add(extId.getPrincipalName());
                    // recursively apply further membership until the configured depth is reached
                    if (depth > 1) {
                        collectPrincipalNames(principalNames, extId.getDeclaredGroups(), depth - 1);
                    }
                } else {
                    log.debug("Not an external group ({}) => ignore.", extId);
                }
            }
        }
    }
}