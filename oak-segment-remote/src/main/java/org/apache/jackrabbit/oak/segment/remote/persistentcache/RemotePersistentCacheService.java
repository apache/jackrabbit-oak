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

import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.segment.spi.monitor.RoleStatisticsProvider;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        configurationPid = {Configuration.PID})
public class RemotePersistentCacheService {
    private ServiceRegistration registration;

    private PersistentCache persistentCache;

    private final Closer closer = Closer.create();

    private OsgiWhiteboard osgiWhiteboard;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        osgiWhiteboard = new OsgiWhiteboard(context.getBundleContext());
        persistentCache = createPersistentCache(config, closer);
        if (persistentCache != null) {
            registration = context.getBundleContext().registerService(PersistentCache.class.getName(), persistentCache, new Properties());
        }
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        closeQuietly(closer);
        persistentCache = null;
    }

    protected void registerCloseable(final Registration registration) {
        closer.register(registration::unregister);
    }

    protected  <T> Registration registerMBean(Class<T> clazz, T bean, String type, String name) {
        return WhiteboardUtils.registerMBean(osgiWhiteboard, clazz, bean, type, name);
    }

    private PersistentCache createPersistentCache(Configuration configuration, Closer closer) {

        RoleStatisticsProvider roleStatisticsProvider = new RoleStatisticsProvider(statisticsProvider, "remote_persistence");

        DiskCacheIOMonitor diskCacheIOMonitor = new DiskCacheIOMonitor(roleStatisticsProvider);
        RedisCacheIOMonitor redisCacheIOMonitor = new RedisCacheIOMonitor(roleStatisticsProvider);

        if (configuration.diskCacheEnabled()) {
            PersistentDiskCache persistentDiskCache = new PersistentDiskCache(new File(configuration.diskCacheDirectory()), configuration.diskCacheMaxSizeMB(), diskCacheIOMonitor);
            closer.register(persistentDiskCache);

            CacheStatsMBean diskCacheStatsMBean = persistentDiskCache.getCacheStats();
            registerCloseable(registerMBean(CacheStatsMBean.class, diskCacheStatsMBean, CacheStats.TYPE, diskCacheStatsMBean.getName()));

            if (configuration.redisCacheEnabled()) {
                PersistentRedisCache redisCache = new PersistentRedisCache(configuration.redisCacheHost(), configuration.redisCachePort(), configuration.redisCacheExpireSeconds(), configuration.redisSocketTimeout(), configuration.redisConnectionTimeout(),
                        configuration.redisMinConnections(), configuration.redisMaxConnections(), configuration.redisMaxTotalConnections(), configuration.redisDBIndex(), redisCacheIOMonitor);
                persistentDiskCache.linkWith(redisCache);
                closer.register(redisCache);

                CacheStatsMBean redisCacheStatsMBean = redisCache.getCacheStats();
                registerCloseable(registerMBean(CacheStatsMBean.class, redisCacheStatsMBean, CacheStats.TYPE, redisCacheStatsMBean.getName()));
            }

            return persistentDiskCache;
        } else if (configuration.redisCacheEnabled()) {
            PersistentRedisCache redisCache = new PersistentRedisCache(configuration.redisCacheHost(), configuration.redisCachePort(), configuration.redisCacheExpireSeconds(), configuration.redisSocketTimeout(), configuration.redisConnectionTimeout(),
                    configuration.redisMinConnections(), configuration.redisMaxConnections(), configuration.redisMaxTotalConnections(), configuration.redisDBIndex(), redisCacheIOMonitor);
            closer.register(redisCache);

            CacheStatsMBean redisCacheStatsMBean = redisCache.getCacheStats();
            registerCloseable(registerMBean(CacheStatsMBean.class, redisCacheStatsMBean, CacheStats.TYPE, redisCacheStatsMBean.getName()));

            return redisCache;
        }

        return null;
    }
}
