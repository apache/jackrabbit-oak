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

import static org.apache.jackrabbit.oak.security.user.CachedPrincipalMembershipReader.CacheConfiguration.EXPIRATION_NO_CACHE;
import static org.apache.jackrabbit.oak.security.user.MembershipCacheConstants.REP_GROUP_PRINCIPAL_NAMES;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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

/**
 * Extension of {@code PrincipalMembershipReader} that caches the membership of a given user principal. The cache
 * behavior has been extracted from {@code UserPrincipalProvider} and improved to address concurrency issues. It will
 * read group principals from the cache if it already exists and is not yet expired. If the cache needs to be written or
 * updated it will verify that no other thread is concurrently writing the cache. If writing the cache is not possible,
 * it will either return a stale cache (which possibly outdated membership information) or read from the membership
 * provider without writing the cache. The handling of stale cache is determined by the new configuration option
 * {@link UserConfigurationImpl.Configuration#cacheMaxStale()} which defaults to
 * {@link CacheConfiguration#NO_STALE_CACHE}.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-10451">OAK-10451</a>
 */
public class CachedPrincipalMembershipReader {

    private static final Logger LOG = LoggerFactory.getLogger(CachedPrincipalMembershipReader.class);

    private static final long MEMBERSHIP_THRESHOLD = 0;

    /**
     * Keep track of cache updates for 100 most recent authorizables.
     */
    private static final int MAX_CACHE_TRACKING_ENTRIES = 100;

    private static final Map<UserConfiguration, Map<String, Long>> CACHE_UPDATES = new ConcurrentHashMap<>();

    private final Function<String, Principal> principalFactory;
    private final UserConfiguration config;

    private final Root root;

    private final long expiration;
    private final long maxStale;
    private final String propertyName;

    CachedPrincipalMembershipReader(@NotNull CacheConfiguration cacheConfiguration,
            @NotNull Root root,
            @NotNull Function<String, Principal> principalFactory) {
        this.expiration = cacheConfiguration.getExpiration();
        this.maxStale = cacheConfiguration.getMaxStale();
        this.propertyName = cacheConfiguration.getPropertyName();
        this.config = cacheConfiguration.getUserConfiguration();
        this.root = root;
        this.principalFactory = principalFactory;
    }

    public Set<Principal> readMembership(@NotNull Tree authorizableTree, Function<Tree, Set<Principal>> cacheLoader) {
        // membership cache only implemented on user
        if (UserUtil.isType(authorizableTree, AuthorizableType.USER)) {
            return readGroupsFromCache(authorizableTree, cacheLoader);
        } else {
            return cacheLoader.apply(authorizableTree);
        }
    }

    private Set<Principal> readGroupsFromCache(@NotNull Tree authorizableTree,
            @NotNull Function<Tree, Set<Principal>> loader) {
        Set<Principal> groups = Collections.emptySet();
        String authorizablePath = authorizableTree.getPath();
        Tree principalCache = authorizableTree.getChild(MembershipCacheConstants.REP_CACHE);
        long expirationTime = readExpirationTime(principalCache);
        long now = System.currentTimeMillis();
        if (isValidCache(expirationTime, now) && principalCache.exists() &&
                !Strings.isNullOrEmpty(TreeUtil.getString(principalCache, propertyName))) {
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
                groups = loader.apply(authorizableTree);
                writeGroupsToCache(authorizableTree, groups);
            } else {
                // another thread is already updating the cache, which leaves two options
                // 1. serve a stale cache if allowed
                // 2. read membership from membership-provider without caching the result
                if (canServeStaleCache(expirationTime, now)) {
                    // the cache cannot be updated by the current thread but a stale cache can be returned and reading
                    // membership again can be avoided
                    LOG.debug("Another thread is updating the cache, returning a stale cache for '{}'.",
                            authorizablePath);
                    groups = serveGroupsFromCache(principalCache);
                } else {
                    // another thread is updating the cache and this thread is not allowed to serve a stale cache,
                    // therefore read membership from membership-provider but do not cache the result.
                    LOG.debug(
                            "Another thread is updating the cache and this thread is not allowed to serve a stale cache; reading from persistence without caching.");
                    groups = loader.apply(authorizableTree);
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
        return groups;
    }

    private static long readExpirationTime(@NotNull Tree principalCache) {
        if (!principalCache.exists()) {
            return EXPIRATION_NO_CACHE;
        }
        return TreeUtil.getLong(principalCache, MembershipCacheConstants.REP_EXPIRATION, EXPIRATION_NO_CACHE);
    }

    private static boolean isValidCache(long expirationTime, long now) {
        return expirationTime > EXPIRATION_NO_CACHE && now < expirationTime;
    }

    private boolean canServeStaleCache(long expirationTime, long now) {
        return now - expirationTime < maxStale;
    }

    /**
     * Populate the given set with the group principals read from the cache.
     *
     * @param principalCache The tree holding the cached group principal names.
     */
    private Set<Principal> serveGroupsFromCache(@NotNull Tree principalCache) {
        String str = TreeUtil.getString(principalCache, this.propertyName);
        if (Strings.isNullOrEmpty(str)) {
            return Collections.emptySet();
        }

        Set<Principal> groups = new HashSet<>();
        for (String s : Text.explode(str, ',')) {
            final String name = Text.unescape(s);
            groups.add(principalFactory.apply(name));
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
            Tree cache = authorizableTree.getChild(MembershipCacheConstants.REP_CACHE);
            String authorizablePath = authorizableTree.getPath();
            if (!cache.exists()) {
                if (groupPrincipals.size() <= MEMBERSHIP_THRESHOLD) {
                    LOG.debug("Omit cache creation for user without membership at {}", authorizablePath);
                    return;
                } else {
                    LOG.debug("Attempting to create new membership cache at {}", authorizablePath);
                    cache = TreeUtil.addChild(authorizableTree,
                            MembershipCacheConstants.REP_CACHE,
                            MembershipCacheConstants.NT_REP_CACHE);
                }
            }
            cache.setProperty(MembershipCacheConstants.REP_EXPIRATION, LongUtils.calculateExpirationTime(expiration));
            String value = (groupPrincipals.isEmpty()) ? "" : Joiner.on(",").join(Iterables.transform(groupPrincipals, input -> Text.escape(input.getName())));
            cache.setProperty(this.propertyName, value);

            root.commit(CacheValidatorProvider.asCommitAttributes());
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

    static class CacheConfiguration {

        static final String PARAM_CACHE_EXPIRATION = UserPrincipalProvider.PARAM_CACHE_EXPIRATION;
        static final String PARAM_CACHE_MAX_STALE = UserPrincipalProvider.PARAM_CACHE_MAX_STALE;
        public static final long EXPIRATION_NO_CACHE = 0;
        public static final long NO_STALE_CACHE = 0;

        private final UserConfiguration userConfig;
        private final long expiration;
        private final long maxStale;
        private final String propertyName;

        private CacheConfiguration(UserConfiguration userConfig, long expiration, long maxStale, @NotNull String propertyName) {
            this.userConfig = userConfig;
            this.expiration = expiration;
            this.maxStale = maxStale;
            this.propertyName = propertyName;
        }

        static CacheConfiguration fromUserConfiguration(UserConfiguration config) {
            return new CacheConfiguration(
                    config,
                    config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE),
                    config.getParameters().getConfigValue(PARAM_CACHE_MAX_STALE, NO_STALE_CACHE),
                    REP_GROUP_PRINCIPAL_NAMES);
        }

        // TODO Do not like this but need to think a better constructor, builder pattern looks overkill
        static CacheConfiguration fromUserConfiguration(UserConfiguration config, @NotNull String propertyName) {
            if (Strings.isNullOrEmpty(propertyName)) {
                throw new IllegalArgumentException("Invalid property name: " + propertyName);
            }
            return new CacheConfiguration(
                    config,
                    config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE),
                    config.getParameters().getConfigValue(PARAM_CACHE_MAX_STALE, NO_STALE_CACHE),
                    propertyName);
        }

        public boolean isCacheEnabled() {
            return expiration > EXPIRATION_NO_CACHE;
        }

        private long getExpiration() {
            return expiration;
        }

        private long getMaxStale() {
            return maxStale;
        }

        private String getPropertyName() {
            return propertyName;
        }

        private UserConfiguration getUserConfiguration() {
            return userConfig;
        }
    }
}
