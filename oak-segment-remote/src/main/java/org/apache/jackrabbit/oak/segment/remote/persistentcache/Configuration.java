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
 *
 */
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import static org.apache.jackrabbit.oak.segment.remote.persistentcache.Configuration.PID;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCache.DEFAULT_MAX_CACHE_SIZE_MB;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentRedisCache.DEFAULT_REDIS_CACHE_EXPIRE_SECONDS;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak Remote Persistent Cache Service",
        description = "Persistent cache for the Oak Segment Node Store")
public @interface Configuration {

    String PID = "org.apache.jackrabbit.oak.segment.remote.RemotePersistentCacheService";

    @AttributeDefinition(
            name = "Disk cache persistence",
            description = "Boolean value indicating that the local disk persisted cache should be used for segment store"
    )
    boolean diskCacheEnabled() default false;

    @AttributeDefinition(
            name = "Disk cache persistence directory",
            description = "Path on the file system where disk cache persistence data will be stored."
    )
    String diskCacheDirectory() default "";

    @AttributeDefinition(
            name = "Disk cache persistence maximum size",
            description = "Disk cache size (in MB). Default value is " + DEFAULT_MAX_CACHE_SIZE_MB
    )
    int diskCacheMaxSizeMB() default DEFAULT_MAX_CACHE_SIZE_MB;

    @AttributeDefinition(
            name = "Redis cache persistence",
            description = "Boolean value indicating that the redis persisted cache should be used for segment store"
    )
    boolean redisCacheEnabled() default false;

    @AttributeDefinition(
            name = "Redis cache persistence host",
            description = "Remote redis server host"
    )
    String redisCacheHost() default "";

    @AttributeDefinition(
            name = "Redis cache persistence port",
            description = "Remote redis server port (0 = default)"
    )
    int redisCachePort() default 0;

    @AttributeDefinition(
            name = "Redis cache persistence entries expiry interval",
            description = "Number of seconds to keep the entries in the cache. Default value is " + DEFAULT_REDIS_CACHE_EXPIRE_SECONDS
    )
    int redisCacheExpireSeconds() default DEFAULT_REDIS_CACHE_EXPIRE_SECONDS;

    @AttributeDefinition(
            name = "Redis cache db index",
            description = "Redis cache db index (see Jedis#select(int))"
    )
    int redisDBIndex() default 1;

    @AttributeDefinition(
            name = "Redis socket timeout",
            description = "Number of seconds to wait for response for request"
    )
    int redisSocketTimeout() default 10;

    @AttributeDefinition(
            name = "Redis connection timeout",
            description = "Number of milliseconds to wait for redis connection to be established"
    )
    int redisConnectionTimeout() default 5000;

    @AttributeDefinition(
            name = "Redis Minimum Connections",
            description = "Minimum number of established connections that need to be kept in the pool"
    )
    int redisMinConnections() default 10;

    @AttributeDefinition(
            name = "Redis Maximum Connections",
            description = "Maximum number of connections required by the business"
    )
    int redisMaxConnections() default 100;

    @AttributeDefinition(
            name = "Redis Maximum Total Connections",
            description = "Includes the number of idle connections as a surplus"
    )
    int redisMaxTotalConnections() default 200;
}
