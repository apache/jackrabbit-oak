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

import com.google.common.collect.Iterables;
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
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

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

    public DynamicSyncContext(@NotNull DefaultSyncConfig config,
                              @NotNull ExternalIdentityProvider idp,
                              @NotNull UserManager userManager,
                              @NotNull ValueFactory valueFactory) {
        super(config, idp, userManager, valueFactory);
    }
    
    public boolean convertToDynamicMembership(@NotNull Authorizable authorizable) throws RepositoryException {
        if (authorizable.isGroup() || !groupsSyncedBefore(authorizable)) {
            return false;
        }
        
        Collection<String> principalNames = clearGroupMembership(authorizable);
        authorizable.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, createValues(principalNames));
        return true;
    }

    //--------------------------------------------------------< SyncContext >---
    @NotNull
    @Override
    public SyncResult sync(@NotNull ExternalIdentity identity) throws SyncException {
        if (identity instanceof ExternalUser) {
            return super.sync(identity);
        } else if (identity instanceof ExternalGroup) {
            ExternalIdentityRef ref = identity.getExternalId();
            if (!isSameIDP(ref)) {
                // create result in accordance with sync(String) where status is FOREIGN
                return new DefaultSyncResultImpl(new DefaultSyncedIdentity(identity.getId(), ref, true, -1), SyncResult.Status.FOREIGN);
            }
            return sync((ExternalGroup) identity, ref);
        } else {
            throw new IllegalArgumentException("identity must be user or group but was: " + identity);
        }
    }
    
    @NotNull
    private SyncResult sync(@NotNull ExternalGroup identity, @NotNull ExternalIdentityRef ref) throws SyncException {
        try {
            Group group = getAuthorizable(identity, Group.class);
            if (group != null) {
                // this group has been synchronized before -> continue updating for consistency.
                return syncGroup(identity, group);
            } else if (hasDynamicGroups()) {
                // group does not exist and dynamic-groups option is enabled -> sync the group
                log.debug("ExternalGroup {}: synchronizing as dynamic group {}.", ref.getString(), identity.getId());
                group = createGroup(identity);
                DefaultSyncResultImpl res = syncGroup(identity, group);
                res.setStatus(SyncResult.Status.ADD);
                return res;
            } else {
                // external group has never been synchronized before and dynamic membership is enabled:
                // don't sync external groups into the repository internal user management
                // but limit synchronized information to group-principals stored
                // separately with each external user such that the subject gets
                // properly populated upon login
                log.debug("ExternalGroup {}: Not synchronized as Group into the repository.", ref.getString());
                return new DefaultSyncResultImpl(new DefaultSyncedIdentity(identity.getId(), ref, true, -1), SyncResult.Status.NOP);
            }
        } catch (RepositoryException e) {
            throw new SyncException(e);
        }
    }

    //-------------------------------------------------< DefaultSyncContext >---
    @Override
    protected void syncMembership(@NotNull ExternalIdentity external, @NotNull Authorizable auth, long depth) throws RepositoryException {
        if (auth.isGroup()) {
            return;
        }

        boolean groupsSyncedBefore = groupsSyncedBefore(auth);
        if (groupsSyncedBefore && !enforceDynamicSync()) {
            // user has been synchronized before dynamic membership has been turned on. continue regular sync unless 
            // either dynamic membership is enforced or dynamic-group option is enabled.
            super.syncMembership(external, auth, depth);
        } else {
            try {
                Iterable<ExternalIdentityRef> declaredGroupRefs = external.getDeclaredGroups();
                // resolve group-refs respecting depth to avoid iterating twice
                Map<ExternalIdentityRef, SyncEntry> map = collectSyncEntries(declaredGroupRefs, depth);
                
                // store dynamic membership with the user
                setExternalPrincipalNames(auth, map.values());
                
                // if dynamic-group option is enabled -> sync groups without member-information
                // in case group-membership has been synched before -> clear it
                if (hasDynamicGroups() && depth > 0) {
                    createDynamicGroups(map.values());
                }
                
                // clean up any other membership
                if (groupsSyncedBefore) {
                    clearGroupMembership(auth);
                }
            } catch (ExternalIdentityException e) {
                log.error("Failed to synchronize membership information for external identity {}", external.getId(), e);
            }
        }
    }

    @Override
    protected void applyMembership(@NotNull Authorizable member, @NotNull Set<String> groups) throws RepositoryException {
        log.debug("Dynamic membership sync enabled => omit setting auto-membership for {} ", member.getID());
    }

    /**
     * Retrieve membership of the given external user (up to the configured depth) and add (or replace) the 
     * rep:externalPrincipalNames property with the accurate collection of principal names.
     * 
     * @param authorizable The target synced user
     * @param syncEntries The set of sync entries collected before.
     * @throws RepositoryException If another error occurs
     */
    private void setExternalPrincipalNames(@NotNull Authorizable authorizable, @NotNull Collection<SyncEntry> syncEntries) throws RepositoryException {
        Value[] vs;
        if (syncEntries.isEmpty()) {
            vs = new Value[0];
        } else {
            Set<String> principalsNames = syncEntries.stream().map(syncEntry -> syncEntry.principalName).collect(Collectors.toSet());
            vs = createValues(principalsNames);
        }
        authorizable.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, vs);
    }
    
    @NotNull
    private Map<ExternalIdentityRef, SyncEntry> collectSyncEntries(@NotNull Iterable<ExternalIdentityRef> declaredGroupRefs, long depth) throws RepositoryException, ExternalIdentityException {
        if (depth <= 0) {
            return Collections.emptyMap();
        }
        Map<ExternalIdentityRef, SyncEntry> map = new HashMap<>();
        collectSyncEntries(declaredGroupRefs, depth, map);
        return map;
    }

    /**
     * Recursively collect the sync entries of the given declared group references up to the given depth.
     *
     * Note, that this method will filter out references that don't belong to the same IDP (see OAK-8665).
     *
     * @param declaredGroupRefs The declared group references for a user or a group.
     * @param depth Configured membership nesting; the recursion will be stopped once depths is < 1.
     * @param map The map to be filled with all group refs and the corresponding sync entries.
     * @throws ExternalIdentityException If an error occurs while resolving the the external group references.
     */
    private void collectSyncEntries(@NotNull Iterable<ExternalIdentityRef> declaredGroupRefs, long depth, @NotNull Map<ExternalIdentityRef, SyncEntry> map) throws ExternalIdentityException, RepositoryException {
        boolean shortcut = shortcut(depth);
        for (ExternalIdentityRef ref : Iterables.filter(declaredGroupRefs, this::isSameIDP)) {
            String principalName = null;
            Authorizable a = null;
            ExternalGroup externalGroup = null;
            if (shortcut) {
                principalName = ((PrincipalNameResolver) idp).fromExternalIdentityRef(ref);
                a = userManager.getAuthorizable(new PrincipalImpl(principalName));
            } else {
                // get group from the IDP
                externalGroup = getExternalGroupFromRef(ref);
                if (externalGroup != null) {
                    // only set principal-name if the ref can be resolved to a valid external group
                    principalName = externalGroup.getPrincipalName();
                    a = userManager.getAuthorizable(externalGroup.getId());

                    // recursively apply further membership until the configured depth is reached
                    if (depth > 1) {
                        collectSyncEntries(externalGroup.getDeclaredGroups(), depth - 1, map);
                    }
                }
            }

            if (principalName != null && !isConflictingGroup(a, principalName)) {
                map.put(ref, new SyncEntry(principalName, externalGroup, (Group) a));
            }
        }
    }

    /**
     * Evaluate if looking up the external group from the IDP can be omitted (i.e. no nesting and IDP implements PrincipalNameResolver.
     * Finally, the shortcut does not make sense if 'dynamic group' option is enabled, as the external group is needed
     * for the subsequent group sync.
     * 
     * @param depth The configured membership nesting depth.
     * @return {@code true} if looking up the external group on IDP can be avoided; {@code false} otherwise.
     */
    private boolean shortcut(long depth) {
        return depth <= 1 && idp instanceof PrincipalNameResolver && !hasDynamicGroups();
    }
    
    /**
     * Tests if the given existing user/group collides with the external group having the same principall name.
     * It is considered a conflict if the existing authorizable is a user (and not a group) or if it doesn't belong to the 
     * same IDP (i.e. a local group or one defined for a different IDP).
     * 
     * NOTE: this method does not verify if the 'rep:authorizableId' or the identifier part of 'rep:externalId' match,
     * Instead it assumes that external identities and synced authorizables that are associated with the same IDP can be 
     * trusted to be consistent as long as they have the same principal name.
     *
     * @param authorizable An authorizable with the given principal name or {@code null}.
     * @return {@code true} if the given user/group collides with the external group with the given principal name; {@code false} otherwise.
     * @throws RepositoryException If an error occurs
     */
    private boolean isConflictingGroup(@Nullable Authorizable authorizable, @NotNull String principalName) throws RepositoryException {
        if (authorizable == null) {
            return false;
        } else if (!authorizable.isGroup()) {
            log.warn("Existing user '{}' collides with external group defined by IDP '{}'.", authorizable.getID(), idp.getName());
            return true;
        } else if (!isSameIDP(authorizable)) {
            // there exists a group with the same id or principal name but it doesn't belong to the same IDP
            // in consistency with DefaultSyncContext don't sync this very membership into the repository
            // and log a warning about the collision instead.
            log.warn("Existing group with id '{}' and principal name '{}' is not defined by IDP '{}'.", authorizable.getID(), authorizable.getPrincipal().getName(), idp.getName());
            return true;
        } else if (!principalName.equals(authorizable.getPrincipal().getName())) {
            // there exists a group with matching ID but principal-mismatch, don't sync this very membership into the 
            // repository and log a warning about the collision instead.
            log.warn("Existing group with id '{}' doesn't have matching principal name. found '{}', expected '{}', IDP '{}'.", authorizable.getID(), authorizable.getPrincipal().getName(), principalName, idp.getName());
            return true;            
        } else {
            // group has been synced before (same IDP, same principal-name)
            return false;
        }
    }

    private void createDynamicGroups(@NotNull Iterable<SyncEntry> syncEntries) throws RepositoryException {
        for (SyncEntry syncEntry : syncEntries) {
            // since 'shortcut' is omitted if dynamic groups are enabled, there is no need to test if 'external-group' is 
            // null, nor trying to retrieve external group again. if it could not be resolved during 'collectSyncEntries'
            // before it didn't got added to the map
            checkNotNull(syncEntry.externalGroup, "Cannot create dynamic group from null ExternalIdentity.");
            
            // lookup of existing group by ID has been performed already including check for conflicting authorizable 
            // type or principal name
            Group gr = syncEntry.group;
            if (gr == null) {
                gr = createGroup(syncEntry.externalGroup);
            }
            syncGroup(syncEntry.externalGroup, gr);
        }
    }
    
    @NotNull
    private Collection<String> clearGroupMembership(@NotNull Authorizable authorizable) throws RepositoryException {
        Set<String> groupPrincipalNames = new HashSet<>();
        Set<Group> toRemove = new HashSet<>();

        // loop over declared and inherited groups as it has been synchronzied before to clean up any previously 
        // defined membership to external groups and automembership.
        // principal-names are collected solely for migration trigger through JXM
        clearGroupMembership(authorizable, groupPrincipalNames, toRemove);
        
        // finally remove external groups that are no longer needed
        for (Group group : toRemove) {
            group.remove();
        }
        return groupPrincipalNames;
    }
    
    private void clearGroupMembership(@NotNull Authorizable authorizable, @NotNull Set<String> groupPrincipalNames, @NotNull Set<Group> toRemove) throws RepositoryException {
        Iterator<Group> grpIter = authorizable.declaredMemberOf();
        Set<String> autoMembership = ((authorizable.isGroup()) ? config.group() : config.user()).getAutoMembership(authorizable);
        while (grpIter.hasNext()) {
            Group grp = grpIter.next();
            if (isSameIDP(grp)) {
                // collected same-idp group principals for the rep:externalPrincipalNames property 
                groupPrincipalNames.add(grp.getPrincipal().getName());
                grp.removeMember(authorizable);
                clearGroupMembership(grp, groupPrincipalNames, toRemove);
                if (clearGroup(grp)) {
                    toRemove.add(grp);
                }
            } else if (autoMembership.contains(grp.getID())) {
                // clear auto-membership
                grp.removeMember(authorizable);
                clearGroupMembership(grp, groupPrincipalNames, toRemove);
            } else {
                // some other membership that has not been added by the sync process
                log.warn("Ignoring unexpected membership of '{}' in group '{}' crossing IDP boundary.", authorizable.getID(), grp.getID());
            }
        }
    }
    
    private boolean hasDynamicGroups() {
        return config.group().getDynamicGroups();
    }
    
    private boolean enforceDynamicSync() {
        return config.user().getEnforceDynamicMembership() || hasDynamicGroups();
    }
    
    private boolean clearGroup(@NotNull Group group) throws RepositoryException {
        if (hasDynamicGroups()) {
            return false;
        } else {
            return !group.getDeclaredMembers().hasNext();
        }
    }
    
    private static boolean groupsSyncedBefore(@NotNull Authorizable authorizable) throws RepositoryException {
        return authorizable.hasProperty(REP_LAST_SYNCED) && !authorizable.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
    }

    /**
     * Helper object to avoid repeated lookup of principalName, {@link ExternalGroup} and synchronized {@link Group} for 
     * a given {@link ExternalIdentityRef} during {@link #syncMembership(ExternalIdentity, Authorizable, long)}.
     */
    private static class SyncEntry {
        
        private final String principalName;
        private final ExternalGroup externalGroup;
        private final Group group;
        
        private SyncEntry(@NotNull String principalName, @Nullable ExternalGroup externalGroup, @Nullable Group group) {
            this.principalName = principalName;
            this.externalGroup = externalGroup;
            this.group = group;
        }
    }
}
