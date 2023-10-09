/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.Principal;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class CacheThreadProvider {

    private static CacheThreadProvider instance = null;
    private static ConcurrentHashMap<String, PrincipalCacheCommitterThread> committerThreadMap = new ConcurrentHashMap<>();

    public static @Nullable CacheThreadProvider getInstance() {
        if (instance == null) {
            instance = new CacheThreadProvider();
        }
        return instance;
    }

    protected synchronized @Nullable PrincipalCacheCommitterThread cacheGroups(@NotNull Tree authorizableNode, @NotNull Set<Principal> groupPrincipals, long expiration, @NotNull Root root) {
        String authorizableNodePath = authorizableNode.getPath();
        if (committerThreadMap.containsKey(authorizableNodePath)) {
            // One thread is already committing. return null to inform the caller that doesn't have to wait for the commit to finish
            return null;
        } else {
            PrincipalCacheCommitterThread committerThread = new PrincipalCacheCommitterThread(authorizableNode, groupPrincipals, expiration, root);
            committerThreadMap.put(authorizableNodePath, committerThread);
            committerThread.start();
            return committerThread;
        }
    }

    protected ConcurrentHashMap<String, PrincipalCacheCommitterThread> getCommitterThreadMap() {
        return committerThreadMap;
    }

    protected void removeCommitterThread(String authorizableNodePath) {
        committerThreadMap.remove(authorizableNodePath);
    }

}
