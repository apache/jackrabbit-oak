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


import static org.apache.jackrabbit.oak.security.user.CacheConfiguration.EXPIRATION_NO_CACHE;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.jcr.AccessDeniedException;
import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheLoader;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachePrincipalFactory;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachedMembershipReader;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of {@code PrincipalMembershipReader} that caches the membership of a given user principal. The cache
 * behavior has been extracted from {@code UserPrincipalProvider} and improved to address concurrency issues. It will
 * read group principals from the cache if it already exists and is not yet expired. If the cache needs to be written or
 * updated it will verify that no other thread is concurrently writing the cache. If writing the cache is not possible,
 * it will either return a stale cache (which possibly outdated membership information) or read from the membership
 * provider without writing the cache. The handling of stale cache is determined by the new configuration option
 * {@code UserConfigurationImpl.Configuration#cacheMaxStale()} which defaults to
 * {@link CacheConfiguration#NO_STALE_CACHE}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-10451">OAK-10451</a>
 */
class CachedPrincipalMembershipReader implements CachedMembershipReader {

    private static final Logger LOG = LoggerFactory.getLogger(CachedPrincipalMembershipReader.class);

    /**
     * Keep track of cache updates for 100 most recent authorizables.
     */
    private static final int MAX_CACHE_TRACKING_ENTRIES = 100;

    private static final Map<UserConfiguration, Map<String, Long>> CACHE_UPDATES = new ConcurrentHashMap<>();

    private final CachePrincipalFactory principalFactory;
    private final UserConfiguration config;

    private final Root root;

    private final long expiration;
    private final long maxStale;
    private final String propertyName;
    private final int membershipThreshold;

    CachedPrincipalMembershipReader(@NotNull CacheConfiguration cacheConfiguration, @NotNull Root root,
            @NotNull CachePrincipalFactory principalFactory) {
        this.expiration = cacheConfiguration.getExpiration();
        this.maxStale = cacheConfiguration.getMaxStale();
        this.propertyName = cacheConfiguration.getPropertyName();
        this.config = cacheConfiguration.getUserConfiguration();
        this.membershipThreshold = cacheConfiguration.getMembershipThreshold();
        this.root = root;
        this.principalFactory = principalFactory;
    }

    @Override
    public Set<Principal> readMembership(@NotNull Tree authorizableTree, CacheLoader cacheLoader) {
        // membership cache only implemented on user
        if (UserUtil.isType(authorizableTree, AuthorizableType.USER)) {
            return readGroupsFromCache(authorizableTree, cacheLoader);
        } else {
            return cacheLoader.load(authorizableTree);
        }
    }

    private Set<Principal> readGroupsFromCache(@NotNull Tree authorizableTree, CacheLoader loader) {
        Set<Principal> groups = Collections.emptySet();
        String authorizablePath = authorizableTree.getPath();
        Tree principalCache = authorizableTree.getChild(CacheConstants.REP_CACHE);
        long expirationTime = readExpirationTime(principalCache);
        long now = System.currentTimeMillis();
        if (isValidCache(expirationTime, now) && hasPropertyCached(principalCache)) {
            LOG.debug("Reading membership from cache for '{}'", authorizablePath);
            return serveGroupsFromCache(principalCache);
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
                groups = loader.load(authorizableTree);
                writeGroupsToCache(authorizableTree, groups);
            } else {
                // another thread is already updating the cache, which leaves two options
                // 1. serve a stale cache if allowed and if the cache values exist
                // 2. read membership from membership-provider without caching the result
                if (canServeStaleCache(expirationTime, now) && hasCacheValues(principalCache)) {
                    // the cache cannot be updated by the current thread but a stale cache can be returned and reading
                    // membership again can be avoided
                    LOG.debug("Another thread is updating the cache, returning a stale cache for '{}'.",
                            authorizablePath);
                    groups = serveGroupsFromCache(principalCache);
                } else {
                    // another thread is updating the cache and this thread is not allowed to serve a stale cache
                    // because there's no cache or it's not allowed to serve stale cache
                    // therefore read membership from membership-provider.
                    LOG.debug("This thread is not allowed to serve a stale cache; reading from provider without caching.");
                    groups = loader.load(authorizableTree);
                }
            }
        } catch (AccessDeniedException | CommitFailedException e) {
            // Failed to write cache but groups should have been loaded successfully
            LOG.debug("Failed to cache membership: {}", e.getMessage());
        } finally {
            // remove current entry from the cache updates map verifying that the current thread is the owner
            // clearing the way for other threads to update the cache if it has expired or persisting the cache has failed.
            synchronized (updates) {
                updates.remove(authorizablePath, marker);
            }
        }
        return groups;
    }

    private static long readExpirationTime(@NotNull Tree principalCache) {
        if (!principalCache.exists()) {
            return EXPIRATION_NO_CACHE;
        }
        return TreeUtil.getLong(principalCache, CacheConstants.REP_EXPIRATION, EXPIRATION_NO_CACHE);
    }

    private static boolean isValidCache(long expirationTime, long now) {
        return expirationTime > EXPIRATION_NO_CACHE && now < expirationTime;
    }

    private boolean canServeStaleCache(long expirationTime, long now) {
        return now - expirationTime < maxStale;
    }

    private boolean hasPropertyCached(Tree principalCache) {
        return principalCache.hasProperty(propertyName);
    }

    private boolean hasCacheValues(@NotNull Tree principalCache) {
        return principalCache.hasProperty(propertyName) &&
                !Strings.isNullOrEmpty(TreeUtil.getString(principalCache, propertyName));
    }

    /**
     * Populate the given set with the group principals read from the cache.
     *
     * @param principalCache The tree holding the cached group principal names.
     */
    private Set<Principal> serveGroupsFromCache(@NotNull Tree principalCache) {
        String str = TreeUtil.getString(principalCache, this.propertyName, "");
        Set<Principal> groups = new HashSet<>();
        for (String s : Text.explode(str, ',')) {
            final String name = Text.unescape(s);
            groups.add(principalFactory.create(name));
        }
        return groups;
    }

    /**
     * Cache the group principal names for the given authorizable in the subtree of the user-node in a dedicated, system
     * maintained rep:cache child node.
     *
     * @param authorizableTree The tree associated with the user.
     * @param groupPrincipals  The set of group principals to cache.
     * @throws AccessDeniedException If the current session does not have the necessary permission to write the cache.
     * @throws CommitFailedException If the cache could not be written to the repository.
     */
    private void writeGroupsToCache(@NotNull Tree authorizableTree,
            @NotNull Set<Principal> groupPrincipals) throws AccessDeniedException, CommitFailedException {
        try {
            root.refresh();
            Tree cache = authorizableTree.getChild(CacheConstants.REP_CACHE);
            String authorizablePath = authorizableTree.getPath();
            if (!cache.exists()) {
                if (groupPrincipals.size() < membershipThreshold) {
                    LOG.debug("Omit cache creation for user without membership at {}", authorizablePath);
                    return;
                } else {
                    LOG.debug("Attempting to create new membership cache at {}", authorizablePath);
                    cache = TreeUtil.addChild(authorizableTree,
                            CacheConstants.REP_CACHE,
                            CacheConstants.NT_REP_CACHE);
                }
            }
            cache.setProperty(CacheConstants.REP_EXPIRATION, LongUtils.calculateExpirationTime(expiration));
            String value = (groupPrincipals.isEmpty()) ? "" : Joiner.on(",").join(Iterables.transform(groupPrincipals, input -> Text.escape(input.getName())));
            cache.setProperty(this.propertyName, value);

            root.commit(CommitMarker.asCommitAttributes());
            LOG.debug("Cached membership property '{}' at {}", propertyName, authorizablePath);
        } finally {
            root.refresh();
        }
    }

    /**
     * Given that every Oak repository instance can be expected to have one {@code UserConfiguration} tied to the
     * {@code SecurityProvider}, synchronize cache updates for each repository separately. Note however, that
     * {@code CACHE_UPDATES} is intended to contain just one single entry for most usages of Apache Jackrabbit Oak.
     * <p>
     * The cache updates map keeps track of the last 100 user path for which the cache is being updated to prevent
     * concurrent updates by multiple threads. The map entries are being removed upon completion of the cache update
     * (even if persisting the changes fails).
     */
    private @NotNull Map<String, Long> getCacheUpdateMap() {
        return CACHE_UPDATES.computeIfAbsent(config, cfg -> new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(@NotNull Map.Entry<String, Long> eldest) {
                return size() > MAX_CACHE_TRACKING_ENTRIES;
            }
        });
    }

    static final class CommitMarker {

        private static final String KEY = CommitMarker.class.getName();

        private static final CommitMarker INSTANCE = new CommitMarker();

        static boolean isValidCommitInfo(@NotNull CommitInfo commitInfo) {
            return CommitMarker.INSTANCE == commitInfo.getInfo().get(CommitMarker.KEY);
        }

        private CommitMarker() {}

        static Map<String, Object> asCommitAttributes() {
            return Collections.singletonMap(CommitMarker.KEY, CommitMarker.INSTANCE);
        }
    }
}
