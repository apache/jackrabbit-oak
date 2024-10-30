/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.spi.security.user.cache;

import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;

import java.security.Principal;
import java.util.Set;

/**
 * Interface for reading the membership information of a given authorizable and store the result in a cache.
 */
public interface CachedMembershipReader {

    /**
     * Read the membership information of the authorizable and store the result in a cache using the provided cacheLoader.
     * @param authorizableTree The authorizable tree for which the membership information should be read.
     * @param cacheLoader The cacheLoader to provide the set of principals to store in the cache.
     * @return The set of principals that are members of the authorizable.
     */
    Set<Principal> readMembership(@NotNull Tree authorizableTree, CacheLoader cacheLoader);
}