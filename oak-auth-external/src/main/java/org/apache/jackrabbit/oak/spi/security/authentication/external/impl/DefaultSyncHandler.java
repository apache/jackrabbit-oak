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

import java.io.ByteArrayInputStream;
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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DefaultSyncHandler} implements an sync handler that synchronizes users and groups from an external identity
 * provider with the repository users.
 * <p/>
 * Please refer to {@link DefaultSyncConfig} for configuration options.
 */
@Component(
        // note that the metatype information is generated from DefaultSyncConfig
        policy = ConfigurationPolicy.REQUIRE
)
@Service
public class DefaultSyncHandler implements SyncHandler {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncHandler.class);

    /**
     * Name of the {@link ExternalIdentity#getExternalId()} property of a synchronized identity.
     */
    public static final String REP_EXTERNAL_ID = "rep:externalId";

    /**
     * Name of the property that stores the time when an identity was synced.
     */
    public static final String REP_LAST_SYNCED = "rep:lastSynced";

    /**
     * internal configuration
     */
    private DefaultSyncConfig config;

    /**
     * Default constructor for OSGi
     */
    @SuppressWarnings("UnusedDeclaration")
    public DefaultSyncHandler() {
    }

    /**
     * Constructor for non-OSGi cases.
     *
     * @param config the configuration
     */
    public DefaultSyncHandler(DefaultSyncConfig config) {
        this.config = config;
    }

    @Activate
    private void activate(Map<String, Object> properties) {
        ConfigurationParameters cfg = ConfigurationParameters.of(properties);
        config = DefaultSyncConfig.of(cfg);
    }

    @Nonnull
    @Override
    public String getName() {
        return config.getName();
    }

    @Nonnull
    @Override
    public SyncContext createContext(@Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager, @Nonnull Root root)
            throws SyncException {
        return new ContextImpl(idp, userManager, root);
    }

    private class ContextImpl implements SyncContext {

        private final ExternalIdentityProvider idp;

        private final UserManager userManager;

        private final ValueFactory valueFactory;

        // we use the same wall clock for the entire context
        private final long now;
        private final Value nowValue;

        private ContextImpl(ExternalIdentityProvider idp, UserManager userManager, Root root) {
            this.idp = idp;
            this.userManager = userManager;
            valueFactory = new ValueFactoryImpl(root, NamePathMapper.DEFAULT);

            // initialize 'now'
            final Calendar nowCal = Calendar.getInstance();
            this.nowValue = valueFactory.createValue(nowCal);
            this.now = nowCal.getTimeInMillis();
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
        public boolean sync(@Nonnull ExternalIdentity identity) throws SyncException {
            try {
                DebugTimer timer = new DebugTimer();
                boolean ret;
                if (identity instanceof ExternalUser) {
                    User user = getAuthorizable(identity, User.class);
                    timer.mark("find");
                    if (user == null) {
                        user = createUser((ExternalUser) identity);
                        timer.mark("create");
                    }
                    ret = syncUser((ExternalUser) identity, user);
                    timer.mark("sync");
                } else if (identity instanceof ExternalGroup) {
                    Group group = getAuthorizable(identity, Group.class);
                    timer.mark("find");
                    if (group == null) {
                        group = createGroup((ExternalGroup) identity);
                        timer.mark("create");
                    }
                    ret = syncGroup((ExternalGroup) identity, group);
                    timer.mark("sync");
                } else {
                    throw new IllegalArgumentException("identity must be user or group but was: " + identity);
                }
                if (log.isDebugEnabled()) {
                    log.debug("sync({}) {}", identity.getId(), timer.getString());
                }
                return ret;
            } catch (RepositoryException e) {
                throw new SyncException(e);
            }
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
        private <T extends Authorizable> T getAuthorizable(@Nonnull ExternalIdentity external, Class<T> type)
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
        @CheckForNull
        private User createUser(ExternalUser externalUser) throws RepositoryException {
            Principal principal = new PrincipalImpl(externalUser.getPrincipalName());
            User user = userManager.createUser(
                    externalUser.getId(),
                    null,
                    principal,
                    joinPaths(config.user().getPathPrefix(), externalUser.getIntermediatePath())
            );
            user.setProperty(REP_EXTERNAL_ID, valueFactory.createValue(externalUser.getExternalId().getString()));
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
        @CheckForNull
        private Group createGroup(ExternalGroup externalGroup) throws RepositoryException {
            Principal principal = new PrincipalImpl(externalGroup.getPrincipalName());
            Group group = userManager.createGroup(
                    externalGroup.getId(),
                    principal,
                    joinPaths(config.group().getPathPrefix(), externalGroup.getIntermediatePath())
            );
            group.setProperty(REP_EXTERNAL_ID, valueFactory.createValue(externalGroup.getExternalId().getString()));
            return group;
        }


        private boolean syncUser(@Nonnull ExternalUser external, @Nonnull User user) throws RepositoryException {
            // first check if user is expired
            // todo: add "forceSync" property for potential background sync
            if (!isExpired(user, config.user().getExpirationTime(), "Properties")) {
                return false;
            }

            // synchronize the properties
            syncProperties(external, user, config.user().getPropertyMapping());

            // synchronize auto-group membership
            applyMembership(user, config.user().getAutoMembership());

            if (isExpired(user, config.user().getMembershipExpirationTime(), "Membership")) {
                // synchronize external memberships
                syncMembership(external, user, config.user().getMembershipNestingDepth());
            }

            // finally "touch" the sync property
            user.setProperty(REP_LAST_SYNCED, nowValue);
            return true;
        }

        private boolean syncGroup(ExternalGroup external, Group group) throws RepositoryException {
            // first check if user is expired
            // todo: add "forceSync" property for potential background sync
            if (!isExpired(group, config.group().getExpirationTime(), "Properties")) {
                return false;
            }

            // synchronize the properties
            syncProperties(external, group, config.group().getPropertyMapping());

            // synchronize auto-group membership
            applyMembership(group, config.group().getAutoMembership());

            // finally "touch" the sync property
            group.setProperty(REP_LAST_SYNCED, nowValue);
            return true;
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
        private void syncMembership(ExternalIdentity external, Authorizable auth, long depth)
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
            timer.mark("existing");

            for (ExternalIdentityRef ref: externalGroups) {
                log.debug("- processing membership {}", ref.getId());
                // get group
                ExternalGroup extGroup;
                try {
                    extGroup = (ExternalGroup) idp.getIdentity(ref);
                } catch (ClassCastException e) {
                    // this should really not be the case, so catching the CCE is ok here.
                    log.warn("External identity '{}' is not a group, but should be one.", ref.getString());
                    continue;
                } catch (ExternalIdentityException e) {
                    log.warn("Unable to retrieve external group '{}' from provider.", ref.getString(), e);
                    continue;
                }
                if (extGroup == null) {
                    log.warn("External group for ref '{}' could not be retrieved from provider.", ref);
                    continue;
                }
                log.debug("- idp returned '{}'", extGroup.getId());

                Group grp;
                try {
                    grp = (Group) userManager.getAuthorizable(extGroup.getId());
                } catch (ClassCastException e) {
                    // this should really not be the case, so catching the CCE is ok here.
                    log.warn("Authorizable '{}' is not a group, but should be one.", extGroup.getId());
                    continue;
                }
                log.debug("- user manager returned '{}'", grp);

                if (grp == null) {
                    grp = createGroup(extGroup);
                    log.debug("- created new group");
                }
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
            for (Group grp: declaredExternalGroups.values()) {
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
        private void applyMembership(Authorizable member, Set<String> groups) throws RepositoryException {
            for (String groupName: groups) {
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
        private void syncProperties(ExternalIdentity ext, Authorizable auth, Map<String, String> mapping)
                throws RepositoryException {
            Map<String, ?> properties = ext.getProperties();
            for (Map.Entry<String, String> entry: mapping.entrySet()) {
                String relPath = entry.getKey();
                String name = entry.getValue();
                Object obj = properties.get(name);
                if (obj == null) {
                    auth.removeProperty(relPath);
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
         * @param auth the authorizable to check
         * @param expirationTime the expiration time to compare to.
         * @param type debug message type
         * @return {@code true} if the authorizable needs sync
         */
        private boolean isExpired(Authorizable auth, long expirationTime, String type) throws RepositoryException {
            Value[] values = auth.getProperty(REP_LAST_SYNCED);
            if (values == null || values.length == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("{} of {} '{}' need sync. " + REP_LAST_SYNCED + " not set.", new Object[] {
                            type,
                            auth.isGroup() ? "group" : "user",
                            auth.getID()
                    });
                }
                return true;
            } else if (now - values[0].getLong() > expirationTime) {
                if (log.isDebugEnabled()) {
                    log.debug("{} of {} '{}' need sync. " + REP_LAST_SYNCED + " expired ({} > {})", new Object[]{
                            type,
                            auth.isGroup() ? "group" : "user",
                            auth.getID(),
                            now - values[0].getLong(), expirationTime
                    });
                }
                return true;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} of {} '{}' do not need sync.", new Object[]{
                            type,
                            auth.isGroup() ? "group" : "user",
                            auth.getID()
                    });
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
        private Value createValue(@Nullable Object v) throws RepositoryException {
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
        private Value[] createValues(Collection<?> propValues) throws RepositoryException {
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
         * todo: allow multiple IDPs on 1 authorizable
         *
         * @param auth the authorizable.
         * @return {@code true} if same IDP.
         */
        private boolean isSameIDP(@Nullable Authorizable auth) throws RepositoryException {
            if (auth == null) {
                return false;
            }
            Value[] v = auth.getProperty(REP_EXTERNAL_ID);
            if (v == null || v.length == 0) {
                return false;
            }
            ExternalIdentityRef ref = ExternalIdentityRef.fromString(v[0].getString());
            return idp.getName().equals(ref.getProviderName());
        }

    }

    /**
     * Robust relative path concatenation.
     * @param paths relative paths
     * @return the concatenated path
     */
    private static String joinPaths(String... paths) {
        StringBuilder result = new StringBuilder();
        for (String path: paths) {
            if (path != null && !path.isEmpty()) {
                int i0 = 0;
                int i1 = path.length();
                while (i0 < i1 && path.charAt(i0) == '/') {
                    i0++;
                }
                while (i1 > i0 && path.charAt(i1-1) == '/') {
                    i1--;
                }
                if (i1 > i0) {
                    if (result.length() > 0) {
                        result.append('/');
                    }
                    result.append(path.substring(i0, i1));
                }
            }
        }
        return result.length() == 0 ? null : result.toString();
    }
}