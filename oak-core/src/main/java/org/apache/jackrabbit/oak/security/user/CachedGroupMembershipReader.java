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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.security.user.CacheConstants.REP_GROUP_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.EXPIRATION_NO_CACHE;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.NO_STALE_CACHE;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.PARAM_CACHE_MAX_STALE;

/**
 * <code>CachedGroupMembershipReader</code>...
 */
class CachedGroupMembershipReader extends GroupMembershipReader {

    private static final Logger LOG = LoggerFactory.getLogger(CachedGroupMembershipReader.class);

    private static final long MEMBERSHIP_THRESHOLD = 0;

    /**
     * Keep track of cache updates for 100 most recent authorizables.
     */
    private static final int MAX_CACHE_TRACKING_ENTRIES = 100;

    private static final Map<UserConfiguration, Map<String, Long>> CACHE_UPDATES = new ConcurrentHashMap<>();

    private final UserConfiguration config;

    private final Root root;

    private final long expiration;
    private final long maxStale;

    CachedGroupMembershipReader(@NotNull MembershipProvider membershipProvider,
                                @NotNull GroupPrincipalFactory groupPrincipalFactory,
                                @NotNull UserConfiguration config,
                                @NotNull Root root) {
        super(membershipProvider, groupPrincipalFactory);
        this.config = checkNotNull(config);
        this.root = root;
        this.expiration = config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE);
        this.maxStale = config.getParameters().getConfigValue(PARAM_CACHE_MAX_STALE, NO_STALE_CACHE);
    }

    @Override
    void getMembership(@NotNull Tree authorizable,
                       @NotNull Set<Principal> groupPrincipals) {
        // membership cache only implemented on user
        if (UserUtil.isType(authorizable, AuthorizableType.USER)) {
            readGroupsFromCache(authorizable, groupPrincipals, this::loadGroupPrincipals);
        } else {
            loadGroupPrincipals(authorizable, groupPrincipals);
        }
    }

    private void readGroupsFromCache(@NotNull Tree authorizableNode,
                                     @NotNull Set<Principal> groups,
                                     @NotNull BiConsumer<Tree, Set<Principal>> loader) {
        long expirationTime = 0;
        String authorizablePath = authorizableNode.getPath();
        Tree principalCache = authorizableNode.getChild(CacheConstants.REP_CACHE);
        if (principalCache.exists()) {
            expirationTime = TreeUtil.getLong(principalCache, CacheConstants.REP_EXPIRATION, EXPIRATION_NO_CACHE);
        }
        long now = System.currentTimeMillis();
        if (expirationTime > EXPIRATION_NO_CACHE && now < expirationTime) {
            serveGroupsFromCache(authorizablePath, principalCache, groups);
            return;
        }

        // need to load or serve stale if allowed
        boolean mayServeStale = now - expirationTime < maxStale;
        boolean updateCache;
        Map<String, Long> updates = getCacheUpdateMap();
        synchronized (updates) {
            // anyone else updating that entry already?
            Long exp = updates.get(authorizablePath);
            updateCache = exp == null || expirationTime > exp;
            if (updateCache) {
                // we do it
                updates.put(authorizablePath, expirationTime);
            }
        }

        if (!updateCache && mayServeStale) {
            LOG.debug("Another thread is updating the cache and we may serve stale");
            serveGroupsFromCache(authorizablePath, principalCache, groups);
            return;
        }

        if (updateCache) {
            boolean cacheSuccess = false;
            try {
                loader.accept(authorizableNode, groups);
                cacheGroups(authorizableNode, groups);
                cacheSuccess = true;
            } catch (AccessDeniedException | CommitFailedException e) {
                LOG.debug("Failed to cache group membership: {}", e.getMessage());
            } finally {
                if (!cacheSuccess || expirationTime == 0) {
                    synchronized (updates) {
                        Long exp = updates.get(authorizablePath);
                        if (Objects.equals(exp, expirationTime)) {
                            updates.remove(authorizablePath);
                        }
                    }
                }
            }
        } else {
            // load but do not cache. happens when another thread is updating
            // the cache and this thread is not allowed to serve stale
            LOG.debug("Load but do not cache. Another thread is updating the cache and this thread is not allowed to serve stale");
            loader.accept(authorizableNode, groups);
        }
    }

    private void serveGroupsFromCache(String authorizablePath,
                                      Tree principalCache,
                                      Set<Principal> groups) {
        LOG.debug("Reading group membership at {}", authorizablePath);

        String str = TreeUtil.getString(principalCache, REP_GROUP_PRINCIPAL_NAMES);
        if (Strings.isNullOrEmpty(str)) {
            return;
        }

        for (String s : Text.explode(str, ',')) {
            final String name = Text.unescape(s);
            groups.add(groupPrincipalFactory.create(name));
        }
    }

    private void cacheGroups(@NotNull Tree authorizableNode,
                             @NotNull Set<Principal> groupPrincipals)
            throws AccessDeniedException, CommitFailedException {
        try {
            root.refresh();
            Tree cache = authorizableNode.getChild(CacheConstants.REP_CACHE);
            if (!cache.exists()) {
                if (groupPrincipals.size() <= MEMBERSHIP_THRESHOLD) {
                    LOG.debug("Omit cache creation for user without group membership at {}", authorizableNode.getPath());
                    return;
                } else {
                    LOG.debug("Create new group membership cache at {}", authorizableNode.getPath());
                    cache = TreeUtil.addChild(authorizableNode, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
                }
            }

            cache.setProperty(CacheConstants.REP_EXPIRATION, LongUtils.calculateExpirationTime(expiration));
            String value = (groupPrincipals.isEmpty()) ? "" : Joiner.on(",").join(Iterables.transform(groupPrincipals, input -> Text.escape(input.getName())));
            cache.setProperty(REP_GROUP_PRINCIPAL_NAMES, value);

            root.commit(CacheValidatorProvider.asCommitAttributes());
            LOG.debug("Cached group membership at {}", authorizableNode.getPath());
        } finally {
            root.refresh();
        }
    }

    private Map<String, Long> getCacheUpdateMap() {
        return CACHE_UPDATES.computeIfAbsent(config, cfg -> new LinkedHashMap<>(){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > MAX_CACHE_TRACKING_ENTRIES;
            }
        });
    }
}
