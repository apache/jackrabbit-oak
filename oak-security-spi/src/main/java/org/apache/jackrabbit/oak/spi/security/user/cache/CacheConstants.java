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
package org.apache.jackrabbit.oak.spi.security.user.cache;

/**
 * Constants for persisted user management related caches. Currently this only
 * includes a basic cache for group principals names that is used to populate
 * the set of {@link java.security.Principal}s as present on the
 * {@link javax.security.auth.Subject} in the commit phase of the authentication.
 */
public interface CacheConstants {

    String NT_REP_CACHE = "rep:Cache";
    String REP_CACHE = "rep:cache";
    String REP_EXPIRATION = "rep:expiration";

    String PARAM_CACHE_EXPIRATION = "cacheExpiration";
    String PARAM_CACHE_MAX_STALE = "cacheMaxStale";
}