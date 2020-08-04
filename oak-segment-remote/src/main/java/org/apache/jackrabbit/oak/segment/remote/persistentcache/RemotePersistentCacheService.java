package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;

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

    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        persistentCache = createPersistentCache(config, closer);
        registration = context.getBundleContext().registerService(PersistentCache.class.getName(), persistentCache, new Properties());
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

    private static PersistentCache createPersistentCache(Configuration configuration, Closer closer) {
        if (configuration.diskCacheEnabled()) {
            PersistentDiskCache persistentDiskCache = new PersistentDiskCache(new File(configuration.diskCacheDirectory()), configuration.diskCacheMaxSizeMB());
            closer.register(persistentDiskCache);

            if (configuration.redisCacheEnabled()) {
                PersistentRedisCache redisCache = new PersistentRedisCache(configuration.redisCacheHost(), configuration.redisCachePort(), configuration.redisCacheExpireSeconds(), configuration.redisSocketTimeout(), configuration.redisConnectionTimeout(),
                        configuration.redisMinConnections(), configuration.redisMaxConnections(), configuration.redisMaxTotalConnections());
                persistentDiskCache.linkWith(redisCache);
                closer.register(redisCache);
            }

            return persistentDiskCache;
        } else if (configuration.redisCacheEnabled()) {
            PersistentRedisCache redisCache = new PersistentRedisCache(configuration.redisCacheHost(), configuration.redisCachePort(), configuration.redisCacheExpireSeconds(), configuration.redisSocketTimeout(), configuration.redisConnectionTimeout(),
                    configuration.redisMinConnections(), configuration.redisMaxConnections(), configuration.redisMaxTotalConnections());
            closer.register(redisCache);

            return redisCache;
        }

        return null;
    }
}
