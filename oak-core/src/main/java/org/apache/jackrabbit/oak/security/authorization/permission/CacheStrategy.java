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
package org.apache.jackrabbit.oak.security.authorization.permission;

interface CacheStrategy {

    /**
     * @return Maximal number of access controlled paths that should be read to populate the cache upon initialization.
     */
    long maxSize();

    /**
     * @param numEntriesSize Value of {@link NumEntries#size} for a given path.
     * @param cnt The latest (estimated) count of number of access controlled paths for the principals processed so far.
     * @return {@code true} if permission entries for the current principal should be read to populate the cache; {@code false} otherwise.
     */
    boolean loadFully(long numEntriesSize, long cnt);

    /**
     * @param cnt The final (estimated) count of number of access controlled paths after having initialized the cache for all principals.
     * @return {@code true} if all entries should (or already have) been read into the cache and a simple lookup can be used;
     * {@code false} if the default cache should be used instead.
     */
    boolean usePathEntryMap(long cnt);

}
