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

import javax.jcr.AccessDeniedException;
import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.apache.jackrabbit.oak.security.user.CacheConstants.REP_GROUP_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.EXPIRATION_NO_CACHE;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.NO_STALE_CACHE;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.security.user.UserPrincipalProvider.PARAM_CACHE_MAX_STALE;

/**
 * Extension of {@code PrincipalMembershipReader} that caches the membership of a given user principal. 
 * The cache behavior has been extracted from {@code UserPrincipalProvider} and improved to address concurrency issues.
 * It will read group principals from the cache if it already exists and is not yet expired. If the cache needs to be
 * written or updated it will verify that no other thread is concurrently writing the cache. If writing the cache is
 * not possible, it will either return a stale cache (which possibly outdated membership information) or read from the 
 * membership provider without writing the cache. The handling of stale cache is determined by the new configuration 
 * option {@link UserConfigurationImpl.Configuration#cacheMaxStale()} which defaults to {@link UserPrincipalProvider#NO_STALE_CACHE}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-10451">OAK-10451</a>
 */
class CachedPrincipalMembershipReader extends PrincipalMembershipReader {

    private static final Logger LOG = LoggerFactory.getLogger(CachedPrincipalMembershipReader.class);

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

    CachedPrincipalMembershipReader(@NotNull MembershipProvider membershipProvider,
                                    @NotNull GroupPrincipalFactory groupPrincipalFactory,
                                    @NotNull UserConfiguration config,
                                    @NotNull Root root) {
        super(membershipProvider, groupPrincipalFactory);
        this.config = config;
        this.root = root;
        this.expiration = config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE);
        this.maxStale = config.getParameters().getConfigValue(PARAM_CACHE_MAX_STALE, NO_STALE_CACHE);
    }

    @Override
    void readMembership(@NotNull Tree authorizableTree,
                        @NotNull Set<Principal> groupPrincipals) {
        // membership cache only implemented on user
        if (UserUtil.isType(authorizableTree, AuthorizableType.USER)) {
            readGroupsFromCache(authorizableTree, groupPrincipals, super::readMembership);
        } else {
            super.readMembership(authorizableTree, groupPrincipals);
        }
    }

    private void readGroupsFromCache(@NotNull Tree authorizableTree,
                                     @NotNull Set<Principal> groups,
                                     @NotNull BiConsumer<Tree, Set<Principal>> loader) {
        String authorizablePath = authorizableTree.getPath();
        Tree principalCache = authorizableTree.getChild(CacheConstants.REP_CACHE);
        long expirationTime = readExpirationTime(principalCache);
        long now = System.currentTimeMillis();
        if (isValidCache(expirationTime, now)) {
            LOG.debug("Reading membership from cache for '{}'", authorizablePath);
            serveGroupsFromCache(principalCache, groups);
            return;
        }

        // the cache is either expired or does not yet exist
        // test if the cache can be updated by the current thread using the thread identifier as marker
        // i.e. verify that no other thread is currently updating the cache for the same user
        Map<String, Long> updates = getCacheUpdateMap();
        long marker = Thread.currentThread().getId();
        try {
            boolean updateCache;
            synchronized (updates) {
                // test if this thread can update the cache by trying to place a record in the updates-map for the given
                // user path. this will prevent other threads from updating the cache for the same user. 
                updateCache = (updates.computeIfAbsent(authorizablePath, key -> marker) == marker);
            }

            if (updateCache) {
                // read membership from membership-provider and persist the result in the cache
                loader.accept(authorizableTree, groups);
                cacheGroups(authorizableTree, groups);
            } else {
                // another thread is already updating the cache, which leaves two options
                // 1. serve a stale cache if allowed and if the cache values exist
                // 2. read membership from membership-provider without caching the result
                if (canServeStaleCache(expirationTime, now) && hasCacheValues(principalCache)) {
                    // the cache cannot be updated by the current thread but a stale cache can be returned and reading
                    // membership again can be avoided
                    LOG.debug("Another thread is updating the cache, returning a stale cache for '{}'.", authorizablePath);
                    serveGroupsFromCache(principalCache, groups);
                } else {
                    // another thread is updating the cache and this thread is not allowed to serve a stale cache
                    // because there's no cache or it's not allowed to serve stale cache
                    // therefore read membership from membership-provider.
                    LOG.debug("This thread is not allowed to serve a stale cache; reading from provider without caching.");
                    loader.accept(authorizableTree, groups);
                }
            }
        } catch (AccessDeniedException | CommitFailedException e) {
            LOG.debug("Failed to cache membership: {}", e.getMessage());
        } finally {
            // remove current entry from the cache updates map verifying that the current thread is the owner
            // clearing the way for other threads to update the cache if it has expired or persisting the cache has failed.
            synchronized (updates) {
                updates.remove(authorizablePath, marker);
            }
        }
    }

    private static long readExpirationTime(@NotNull Tree principalCache) {
        if (!principalCache.exists()) {
            return EXPIRATION_NO_CACHE;
        }
        return TreeUtil.getLong(principalCache, CacheConstants.REP_EXPIRATION, EXPIRATION_NO_CACHE);
    }

    private static boolean isValidCache(long expirationTime, long now)  {
        return expirationTime > EXPIRATION_NO_CACHE && now < expirationTime;
    }

    private boolean canServeStaleCache(long expirationTime, long now) {
        return now - expirationTime < maxStale;
    }

    private boolean hasCacheValues(@NotNull Tree principalCache) {
        return principalCache.exists() &&
                !Strings.isNullOrEmpty(TreeUtil.getString(principalCache, REP_GROUP_PRINCIPAL_NAMES));
    }

    /**
     * Populate the given set with the group principals read from the cache.
     *
     * @param principalCache The tree holding the cached group principal names.
     * @param groups The set to populate with the group principals.
     */
    private void serveGroupsFromCache(@NotNull Tree principalCache,
                                      @NotNull Set<Principal> groups) {
        String str = TreeUtil.getString(principalCache, REP_GROUP_PRINCIPAL_NAMES);
        if (Strings.isNullOrEmpty(str)) {
            return;
        }

        for (String s : Text.explode(str, ',')) {
            final String name = Text.unescape(s);
            groups.add(getGroupPrincipalFactory().create(name));
        }
    }

    /**
     * Cache the group principal names for the given authorizable in the subtree of the user-node in a dedicated,
     * system maintained rep:cache child node.
     *
     * @param authorizableTree The tree associated with the user.
     * @param groupPrincipals The set of group principals to cache.
     * @throws AccessDeniedException
     * @throws CommitFailedException
     */
    private void cacheGroups(@NotNull Tree authorizableTree,
                             @NotNull Set<Principal> groupPrincipals) throws AccessDeniedException, CommitFailedException {
        try {
            root.refresh();
            Tree cache = authorizableTree.getChild(CacheConstants.REP_CACHE);
            String authorizablePath = authorizableTree.getPath();
            if (!cache.exists()) {
                if (groupPrincipals.size() <= MEMBERSHIP_THRESHOLD) {
                    LOG.debug("Omit cache creation for user without membership at {}", authorizablePath);
                    return;
                } else {
                    LOG.debug("Attempting to create new membership cache at {}", authorizablePath);
                    cache = TreeUtil.addChild(authorizableTree, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
                }
            }

            cache.setProperty(CacheConstants.REP_EXPIRATION, LongUtils.calculateExpirationTime(expiration));
            String value = (groupPrincipals.isEmpty()) ? "" : Joiner.on(",").join(Iterables.transform(groupPrincipals, input -> Text.escape(input.getName())));
            cache.setProperty(REP_GROUP_PRINCIPAL_NAMES, value);

            root.commit(CacheValidatorProvider.asCommitAttributes());
            LOG.debug("Cached membership at {}", authorizablePath);
        } finally {
            root.refresh();
        }
    }

    /**
     * Given that every Oak repository instance can be expected to have one {@code UserConfiguration} tied to the 
     * {@code SecurityProvider}, synchronize cache updates for each repository separately. Note however, that {@code CACHE_UPDATES}
     * is intended to contain just one single entry for most usages of Apache Jackrabbit Oak.
     *
     * The cache updates map keeps track of the last 100 user path for which the cache is 
     * being updated to prevent concurrent updates by multiple threads. The map entries are being removed upon completion
     * of the cache update (even if persisting the changes fails).
     */
    private @NotNull Map<String, Long> getCacheUpdateMap() {
        return CACHE_UPDATES.computeIfAbsent(config, cfg -> new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(@NotNull Map.Entry<String, Long> eldest) {
                return size() > MAX_CACHE_TRACKING_ENTRIES;
            }
        });
    }
}
