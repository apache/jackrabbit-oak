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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.commons.DebugTimer;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of the sync context
 */
public class DefaultSyncContext implements SyncContext {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncContext.class);

    /**
     * Name of the {@link ExternalIdentity#getExternalId()} property of a synchronized identity.
     */
    public static final String REP_EXTERNAL_ID = "rep:externalId";

    /**
     * Name of the property that stores the time when an identity was synced.
     */
    public static final String REP_LAST_SYNCED = "rep:lastSynced";

    protected final DefaultSyncConfig config;

    protected final ExternalIdentityProvider idp;

    protected final UserManager userManager;

    protected final ValueFactory valueFactory;

    protected boolean keepMissing;

    protected boolean forceUserSync;

    protected boolean forceGroupSync;

    // we use the same wall clock for the entire context
    protected final long now;

    protected final Value nowValue;

    public DefaultSyncContext(@Nonnull DefaultSyncConfig config, @Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager, @Nonnull ValueFactory valueFactory) {
        this.config = config;
        this.idp = idp;
        this.userManager = userManager;
        this.valueFactory = valueFactory;

        // initialize 'now'
        final Calendar nowCal = Calendar.getInstance();
        this.nowValue = valueFactory.createValue(nowCal);
        this.now = nowCal.getTimeInMillis();
    }

    /**
     * Creates a synced identity from the given authorizable.
     * @param auth the authorizable
     * @return the id
     * @throws RepositoryException if an error occurs
     */
    @CheckForNull
    public static DefaultSyncedIdentity createSyncedIdentity(@Nullable Authorizable auth) throws RepositoryException {
        if (auth == null) {
            return null;
        }
        ExternalIdentityRef ref = getIdentityRef(auth);
        Value[] lmValues = auth.getProperty(REP_LAST_SYNCED);
        long lastModified = -1;
        if (lmValues != null && lmValues.length > 0) {
            lastModified = lmValues[0].getLong();
        }
        return new DefaultSyncedIdentity(auth.getID(), ref, auth.isGroup(), lastModified);
    }

    /**
     * Retrieves the external identity ref from the authorizable
     * @param auth the authorizable
     * @return the ref
     * @throws RepositoryException if an error occurs
     */
    @CheckForNull
    public static ExternalIdentityRef getIdentityRef(@Nullable Authorizable auth) throws RepositoryException {
        if (auth == null) {
            return null;
        }
        Value[] v = auth.getProperty(REP_EXTERNAL_ID);
        if (v == null || v.length == 0) {
            return null;
        }
        return ExternalIdentityRef.fromString(v[0].getString());
    }

    /**
     * Robust relative path concatenation.
     * @param paths relative paths
     * @return the concatenated path
     * @deprecated Since Oak 1.3.10. Please use {@link PathUtils#concatRelativePaths(String...)} instead.
     */
    public static String joinPaths(String... paths) {
        return PathUtils.concatRelativePaths(paths);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isKeepMissing() {
        return keepMissing;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public SyncContext setKeepMissing(boolean keepMissing) {
        this.keepMissing = keepMissing;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isForceUserSync() {
        return forceUserSync;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public SyncContext setForceUserSync(boolean forceUserSync) {
        this.forceUserSync = forceUserSync;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isForceGroupSync() {
        return forceGroupSync;
    }

    @Override
    @Nonnull
    public SyncContext setForceGroupSync(boolean forceGroupSync) {
        this.forceGroupSync = forceGroupSync;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public SyncResult sync(@Nonnull ExternalIdentity identity) throws SyncException {
        ExternalIdentityRef ref = identity.getExternalId();
        if (!isSameIDP(ref)) {
            // create result in accordance with sync(String) where status is FOREIGN
            boolean isGroup = (identity instanceof ExternalGroup);
            return new DefaultSyncResultImpl(new DefaultSyncedIdentity(identity.getId(), ref, isGroup, -1), SyncResult.Status.FOREIGN);
        }
        try {
            DebugTimer timer = new DebugTimer();
            DefaultSyncResultImpl ret;
            boolean created = false;
            if (identity instanceof ExternalUser) {
                User user = getAuthorizable(identity, User.class);
                timer.mark("find");
                if (user == null) {
                    user = createUser((ExternalUser) identity);
                    timer.mark("create");
                    created = true;
                }
                ret = syncUser((ExternalUser) identity, user);
                timer.mark("sync");
            } else if (identity instanceof ExternalGroup) {
                Group group = getAuthorizable(identity, Group.class);
                timer.mark("find");
                if (group == null) {
                    group = createGroup((ExternalGroup) identity);
                    timer.mark("create");
                    created = true;
                }
                ret = syncGroup((ExternalGroup) identity, group);
                timer.mark("sync");
            } else {
                throw new IllegalArgumentException("identity must be user or group but was: " + identity);
            }
            if (log.isDebugEnabled()) {
                log.debug("sync({}) -> {} {}", ref.getString(), identity.getId(), timer.getString());
            }
            if (created) {
                ret.setStatus(SyncResult.Status.ADD);
            }
            return ret;
        } catch (RepositoryException e) {
            throw new SyncException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public SyncResult sync(@Nonnull String id) throws SyncException {
        try {
            DebugTimer timer = new DebugTimer();
            DefaultSyncResultImpl ret;
            // find authorizable
            Authorizable auth = userManager.getAuthorizable(id);
            if (auth == null) {
                return new DefaultSyncResultImpl(new DefaultSyncedIdentity(id, null, false, -1), SyncResult.Status.NO_SUCH_AUTHORIZABLE);
            }
            // check if we need to deal with this authorizable
            ExternalIdentityRef ref = getIdentityRef(auth);
            if (ref == null || !isSameIDP(ref)) {
                return new DefaultSyncResultImpl(new DefaultSyncedIdentity(id, ref, auth.isGroup(), -1), SyncResult.Status.FOREIGN);
            }

            if (auth.isGroup()) {
                ExternalGroup external = idp.getGroup(id);
                timer.mark("retrieve");
                if (external == null) {
                    ret = handleMissingIdentity(id, auth, timer);
                } else {
                    ret = syncGroup(external, (Group) auth);
                    timer.mark("sync");
                }
            } else {
                ExternalUser external = idp.getUser(id);
                timer.mark("retrieve");
                if (external == null) {
                    ret = handleMissingIdentity(id, auth, timer);
                } else {
                    ret = syncUser(external, (User) auth);
                    timer.mark("sync");
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("sync({}) -> {} {}", id, ref.getString(), timer.getString());
            }
            return ret;
        } catch (RepositoryException e) {
            throw new SyncException(e);
        } catch (ExternalIdentityException e) {
            throw new SyncException(e);
        }
    }

    private DefaultSyncResultImpl handleMissingIdentity(@Nonnull String id,
                                                        @Nonnull Authorizable authorizable,
                                                        @Nonnull DebugTimer timer) throws RepositoryException {
        DefaultSyncedIdentity syncId = createSyncedIdentity(authorizable);
        SyncResult.Status status;
        if (authorizable.isGroup() && ((Group) authorizable).getDeclaredMembers().hasNext()) {
            log.info("won't remove local group with members: {}", id);
            status = SyncResult.Status.NOP;
        } else if (!keepMissing) {
            if (config.user().getDisableMissing() && !authorizable.isGroup()) {
                ((User) authorizable).disable("No longer exists on external identity provider '" + idp.getName() + "'");
                log.debug("disabling user '{}' that no longer exists on IDP {}", id, idp.getName());
                status = SyncResult.Status.DISABLE;
            } else {
                authorizable.remove();
                log.debug("removing authorizable '{}' that no longer exists on IDP {}", id, idp.getName());
                status = SyncResult.Status.DELETE;
            }
            timer.mark("remove");
        } else {
            status = SyncResult.Status.MISSING;
            log.info("external identity missing for {}, but purge == false.", id);
        }
        return new DefaultSyncResultImpl(syncId, status);
    }

    /**
     * Retrieves the repository authorizable that corresponds to the given external identity
     * @param external the external identity
     * @param type the authorizable type
     * @return the repository authorizable or {@code null} if not found.
     * @throws RepositoryException if an error occurs.
     * @throws SyncException if the repository contains a colliding authorizable with the same name.
     */
    @CheckForNull
    protected <T extends Authorizable> T getAuthorizable(@Nonnull ExternalIdentity external, @Nonnull Class<T> type)
            throws RepositoryException, SyncException {
        Authorizable authorizable = userManager.getAuthorizable(external.getId());
        if (authorizable == null) {
            authorizable = userManager.getAuthorizable(external.getPrincipalName());
        }
        if (authorizable == null) {
            return null;
        } else if (type.isInstance(authorizable)) {
            //noinspection unchecked
            return (T) authorizable;
        } else {
            log.error("Unable to process external {}: {}. Colliding authorizable exists in repository.", type.getSimpleName(), external.getId());
            throw new SyncException("Unexpected authorizable: " + authorizable);
        }
    }

    /**
     * Creates a new repository user for the given external one.
     * Note that this method only creates the authorizable but does not perform any synchronization.
     *
     * @param externalUser the external user
     * @return the repository user
     * @throws RepositoryException if an error occurs
     */
    @Nonnull
    protected User createUser(@Nonnull ExternalUser externalUser) throws RepositoryException {
        Principal principal = new PrincipalImpl(externalUser.getPrincipalName());
        String authId = config.user().isApplyRFC7613UsernameCaseMapped() ?
                        java.text.Normalizer.normalize(externalUser.getId().toLowerCase(), java.text.Normalizer.Form.NFKC) : externalUser.getId();
        User user = userManager.createUser(
                authId,
                null,
                principal,
                PathUtils.concatRelativePaths(config.user().getPathPrefix(), externalUser.getIntermediatePath())
        );
        setExternalId(user, externalUser);
        return user;
    }

    /**
     * Creates a new repository group for the given external one.
     * Note that this method only creates the authorizable but does not perform any synchronization.
     *
     * @param externalGroup the external group
     * @return the repository group
     * @throws RepositoryException if an error occurs
     */
    @Nonnull
    protected Group createGroup(@Nonnull ExternalGroup externalGroup) throws RepositoryException {
        Principal principal = new PrincipalImpl(externalGroup.getPrincipalName());
        Group group = userManager.createGroup(
                externalGroup.getId(),
                principal,
                PathUtils.concatRelativePaths(config.group().getPathPrefix(), externalGroup.getIntermediatePath())
        );
        setExternalId(group, externalGroup);
        return group;
    }

    /**
     * Sets the {@link #REP_EXTERNAL_ID} as obtained from {@code externalIdentity}
     * to the specified {@code authorizable} (user or group). The property is
     * a single value of type {@link javax.jcr.PropertyType#STRING STRING}.
     *
     * @param authorizable The user or group that needs to get the {@link #REP_EXTERNAL_ID} property set.
     * @param externalIdentity The {@link ExternalIdentity} from which to retrieve the value of the property.
     * @throws RepositoryException If setting the property using {@link Authorizable#setProperty(String, Value)} fails.
     */
    private void setExternalId(@Nonnull Authorizable authorizable, @Nonnull ExternalIdentity externalIdentity) throws RepositoryException {
        log.debug("Fallback: setting rep:externalId without adding the corresponding mixin type");
        authorizable.setProperty(REP_EXTERNAL_ID, valueFactory.createValue(externalIdentity.getExternalId().getString()));
    }

    @Nonnull
    protected DefaultSyncResultImpl syncUser(@Nonnull ExternalUser external, @Nonnull User user) throws RepositoryException {
        // make also sure the local user to be synced belongs to the same IDP. Note: 'external' has been verified before.
        if (!isSameIDP(user)) {
            return new DefaultSyncResultImpl(new DefaultSyncedIdentity(external.getId(), external.getExternalId(), false, -1), SyncResult.Status.FOREIGN);
        }

        SyncResult.Status status;
        // check if user is expired
        if (!forceUserSync && !isExpired(user)) {
            status = SyncResult.Status.NOP;
        } else {
            syncExternalIdentity(external, user, config.user());
            if (isExpired(user, config.user().getMembershipExpirationTime(), "Membership")) {
                // synchronize external memberships
                syncMembership(external, user, config.user().getMembershipNestingDepth());
            }
            if (this.config.user().getDisableMissing() && user.isDisabled()) {
                status = SyncResult.Status.ENABLE;
                user.disable(null);
            } else {
                status = SyncResult.Status.UPDATE;
            }
            // finally "touch" the sync property
            user.setProperty(REP_LAST_SYNCED, nowValue);
        }
        return new DefaultSyncResultImpl(createSyncedIdentity(user), status);
    }

    @Nonnull
    protected DefaultSyncResultImpl syncGroup(@Nonnull ExternalGroup external, @Nonnull Group group) throws RepositoryException {
        // make also sure the local user to be synced belongs to the same IDP. Note: 'external' has been verified before.
        if (!isSameIDP(group)) {
            return new DefaultSyncResultImpl(new DefaultSyncedIdentity(external.getId(), external.getExternalId(), false, -1), SyncResult.Status.FOREIGN);
        }

        SyncResult.Status status;
        // first check if group is expired
        if (!forceGroupSync && !isExpired(group)) {
            status = SyncResult.Status.NOP;
        } else {
            syncExternalIdentity(external, group, config.group());
            // finally "touch" the sync property
            group.setProperty(REP_LAST_SYNCED, nowValue);
            status = SyncResult.Status.UPDATE;
        }
        return new DefaultSyncResultImpl(createSyncedIdentity(group), status);
    }

    /**
     * Synchronize content common to both external users and external groups:
     * - properties
     * - auto-group membership
     *
     * @param external The external identity
     * @param authorizable The corresponding repository user/group
     * @param config The sync configuration
     * @throws RepositoryException If an error occurs.
     */
    private void syncExternalIdentity(@Nonnull ExternalIdentity external,
                                      @Nonnull Authorizable authorizable,
                                      @Nonnull DefaultSyncConfig.Authorizable config) throws RepositoryException {
        syncProperties(external, authorizable, config.getPropertyMapping());
        applyMembership(authorizable, config.getAutoMembership());
    }

    /**
     * Recursively sync the memberships of an authorizable up-to the specified depth. If the given depth
     * is equal or less than 0, no syncing is performed.
     *
     * @param external the external identity
     * @param auth the authorizable
     * @param depth recursion depth.
     * @throws RepositoryException
     */
    protected void syncMembership(@Nonnull ExternalIdentity external, @Nonnull Authorizable auth, long depth)
            throws RepositoryException {
        if (depth <= 0) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Syncing membership '{}' -> '{}'", external.getExternalId().getString(), auth.getID());
        }

        final DebugTimer timer = new DebugTimer();
        Iterable<ExternalIdentityRef> externalGroups;
        try {
            externalGroups = external.getDeclaredGroups();
        } catch (ExternalIdentityException e) {
            log.error("Error while retrieving external declared groups for '{}'", external.getId(), e);
            return;
        }
        timer.mark("fetching");

        // first get the set of the existing groups that are synced ones
        Map<String, Group> declaredExternalGroups = new HashMap<String, Group>();
        Iterator<Group> grpIter = auth.declaredMemberOf();
        while (grpIter.hasNext()) {
            Group grp = grpIter.next();
            if (isSameIDP(grp)) {
                declaredExternalGroups.put(grp.getID(), grp);
            }
        }
        timer.mark("reading");

        for (ExternalIdentityRef ref : externalGroups) {
            log.debug("- processing membership {}", ref.getId());
            // get group
            ExternalGroup extGroup;
            try {
                ExternalIdentity extId = idp.getIdentity(ref);
                if (extId instanceof ExternalGroup) {
                    extGroup = (ExternalGroup) extId;
                } else {
                    log.warn("No external group found for ref '{}'.", ref.getString());
                    continue;
                }
            } catch (ExternalIdentityException e) {
                log.warn("Unable to retrieve external group '{}' from provider.", ref.getString(), e);
                continue;
            }
            log.debug("- idp returned '{}'", extGroup.getId());

            Group grp;
            Authorizable a = userManager.getAuthorizable(extGroup.getId());
            if (a == null) {
                grp = createGroup(extGroup);
                log.debug("- created new group");
            } else if (a.isGroup() && isSameIDP(a)) {
                grp = (Group) a;
            } else {
                log.warn("Existing authorizable '{}' is not a group from this IDP '{}'.", extGroup.getId(), idp.getName());
                continue;
            }
            log.debug("- user manager returned '{}'", grp);

            syncGroup(extGroup, grp);

            // ensure membership
            grp.addMember(auth);
            log.debug("- added '{}' as member to '{}'", auth, grp);

            // remember the declared group
            declaredExternalGroups.remove(grp.getID());

            // recursively apply further membership
            if (depth > 1) {
                log.debug("- recursively sync group membership of '{}' (depth = {}).", grp.getID(), depth);
                syncMembership(extGroup, grp, depth - 1);
            } else {
                log.debug("- group nesting level for '{}' reached", grp.getID());
            }
        }
        timer.mark("adding");

        // remove us from the lost membership groups
        for (Group grp : declaredExternalGroups.values()) {
            grp.removeMember(auth);
            log.debug("- removing member '{}' for group '{}'", auth.getID(), grp.getID());
        }
        if (log.isDebugEnabled()) {
            timer.mark("removing");
            log.debug("syncMembership({}) {}", external.getId(), timer.getString());
        }
    }

    /**
     * Ensures that the given authorizable is member of the specific groups. Note that it does not create groups
     * if missing, nor remove memberships of groups not in the given set.
     * @param member the authorizable
     * @param groups set of groups.
     */
    protected void applyMembership(@Nonnull Authorizable member, @Nonnull Set<String> groups) throws RepositoryException {
        for (String groupName : groups) {
            Authorizable group = userManager.getAuthorizable(groupName);
            if (group == null) {
                log.warn("Unable to apply auto-membership to {}. No such group: {}", member.getID(), groupName);
            } else if (group instanceof Group) {
                ((Group) group).addMember(member);
            } else {
                log.warn("Unable to apply auto-membership to {}. Authorizable '{}' is not a group.", member.getID(), groupName);
            }
        }
    }

    /**
     * Syncs the properties specified in the {@code mapping} from the external identity to the given authorizable.
     * Note that this method does not check for value equality and just blindly copies or deletes the properties.
     *
     * @param ext external identity
     * @param auth the authorizable
     * @param mapping the property mapping
     * @throws RepositoryException if an error occurs
     */
    protected void syncProperties(@Nonnull ExternalIdentity ext, @Nonnull Authorizable auth, @Nonnull Map<String, String> mapping)
            throws RepositoryException {
        Map<String, ?> properties = ext.getProperties();
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            String relPath = entry.getKey();
            String name = entry.getValue();
            Object obj = properties.get(name);
            if (obj == null) {
                int nameLen = name.length();
                if (nameLen > 1 && name.charAt(0) == '"' && name.charAt(nameLen-1) == '"') {
                    auth.setProperty(relPath, valueFactory.createValue(name.substring(1, nameLen - 1)));
                } else {
                    auth.removeProperty(relPath);
                }
            } else {
                if (obj instanceof Collection) {
                    auth.setProperty(relPath, createValues((Collection) obj));
                } else if (obj instanceof byte[] || obj instanceof char[]) {
                    auth.setProperty(relPath, createValue(obj));
                } else if (obj instanceof Object[]) {
                    auth.setProperty(relPath, createValues(Arrays.asList((Object[]) obj)));
                } else {
                    auth.setProperty(relPath, createValue(obj));
                }
            }
        }
    }

    /**
     * Checks if the given authorizable needs syncing based on the {@link #REP_LAST_SYNCED} property.
     *
     * @param authorizable the authorizable to check
     * @return {@code true} if the authorizable needs sync
     */
    private boolean isExpired(@Nonnull Authorizable authorizable) throws RepositoryException {
        long expTime = (authorizable.isGroup()) ? config.group().getExpirationTime() : config.user().getExpirationTime();
        return isExpired(authorizable, expTime, "Properties");
    }

    /**
     * Checks if the given authorizable needs syncing based on the {@link #REP_LAST_SYNCED} property.
     * @param auth the authorizable to check
     * @param expirationTime the expiration time to compare to.
     * @param type debug message type
     * @return {@code true} if the authorizable needs sync
     */
    protected boolean isExpired(@Nonnull Authorizable auth, long expirationTime, @Nonnull String type) throws RepositoryException {
        Value[] values = auth.getProperty(REP_LAST_SYNCED);
        if (values == null || values.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} of {} '{}' need sync. " + REP_LAST_SYNCED + " not set.",
                        type, auth.isGroup() ? "group" : "user", auth.getID());
            }
            return true;
        } else if (now - values[0].getLong() > expirationTime) {
            if (log.isDebugEnabled()) {
                log.debug("{} of {} '{}' need sync. " + REP_LAST_SYNCED + " expired ({} > {})",
                        type, auth.isGroup() ? "group" : "user", auth.getID(), now - values[0].getLong(), expirationTime);
            }
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("{} of {} '{}' do not need sync.", type, auth.isGroup() ? "group" : "user", auth.getID());
            }
            return false;
        }
    }

    /**
     * Creates a new JCR value of the given object, checking the internal type.
     * @param v the value
     * @return the JCR value or null
     * @throws RepositoryException if an error occurs
     */
    @CheckForNull
    protected Value createValue(@Nullable Object v) throws RepositoryException {
        if (v == null) {
            return null;
        } else if (v instanceof Boolean) {
            return valueFactory.createValue((Boolean) v);
        } else if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return valueFactory.createValue(((Number) v).longValue());
        } else if (v instanceof Float || v instanceof Double) {
            return valueFactory.createValue(((Number) v).doubleValue());
        } else if (v instanceof BigDecimal) {
            return valueFactory.createValue((BigDecimal) v);
        } else if (v instanceof Calendar) {
            return valueFactory.createValue((Calendar) v);
        } else if (v instanceof Date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date) v);
            return valueFactory.createValue(cal);
        } else if (v instanceof byte[]) {
            Binary bin = valueFactory.createBinary(new ByteArrayInputStream((byte[])v));
            return valueFactory.createValue(bin);
        } else if (v instanceof Binary) {
            return valueFactory.createValue((Binary) v);
        } else if (v instanceof InputStream) {
            return valueFactory.createValue((InputStream) v);
        } else if (v instanceof char[]) {
            return valueFactory.createValue(new String((char[]) v));
        } else {
            return valueFactory.createValue(String.valueOf(v));
        }
    }

    /**
     * Creates an array of JCR values based on the type.
     * @param propValues the given values
     * @return and array of JCR values
     * @throws RepositoryException if an error occurs
     */
    @CheckForNull
    protected Value[] createValues(@Nonnull Collection<?> propValues) throws RepositoryException {
        List<Value> values = new ArrayList<Value>();
        for (Object obj : propValues) {
            Value v = createValue(obj);
            if (v != null) {
                values.add(v);
            }
        }
        return values.toArray(new Value[values.size()]);
    }

    /**
     * Checks if the given authorizable was synced from the same IDP by comparing the IDP name of the
     * {@value #REP_EXTERNAL_ID} property.
     *
     * @param auth the authorizable.
     * @return {@code true} if same IDP.
     */
    protected boolean isSameIDP(@Nullable Authorizable auth) throws RepositoryException {
        ExternalIdentityRef ref = getIdentityRef(auth);
        return ref != null && idp.getName().equals(ref.getProviderName());
    }

    /**
     * Tests if the given {@link ExternalIdentityRef} refers to the same IDP
     * as associated with this context instance.
     *
     * @param ref The {@link ExternalIdentityRef} to be tested.
     * @return {@code true} if {@link ExternalIdentityRef#getProviderName()} refers
     * to the IDP associated with this context instance.
     */
    protected boolean isSameIDP(@Nonnull ExternalIdentityRef ref) {
        return idp.getName().equals(ref.getProviderName());
    }
}