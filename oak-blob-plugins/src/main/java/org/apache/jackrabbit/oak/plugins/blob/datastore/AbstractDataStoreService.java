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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.PROP_SPLIT_BLOBSTORE;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(componentAbstract = true)
public abstract class AbstractDataStoreService {
    private static final String PROP_HOME = "repository.home";
    private static final String PATH = "path";
    public static final String PROP_ENCODE_LENGTH = "encodeLengthInId";
    public static final String PROP_CACHE_SIZE = "cacheSizeInMB";
    private static final String DESCRIPTION = "oak.blobstore.description";

    private static final Logger log = LoggerFactory.getLogger(AbstractDataStoreService.class);

    @Reference
    private StatisticsProvider statisticsProvider;

    protected Closer closer = Closer.create();

    protected void activate(ComponentContext context, Map<String, Object> config) throws RepositoryException {
        // change to mutable map. may be modified in createDS call
        config = Maps.newHashMap(config);

        DataStore ds = createDataStore(context, config);
        registerDataStore(context, config, ds, getStatisticsProvider(), getDescription(), closer);
    }

    protected void deactivate() throws DataStoreException {
        if (null != closer) {
            synchronized (this) {
                if (null != closer) {
                    closeQuietly(closer);
                    closer = null;
                }
            }
        }
    }

    protected abstract DataStore createDataStore(ComponentContext context, Map<String, Object> config);

    protected StatisticsProvider getStatisticsProvider(){
        return statisticsProvider;
    }

    public static DataStore registerDataStore(
            @Nonnull ComponentContext context,
            @Nonnull Map<String, Object> config,
            @Nullable DataStore ds,
            @Nonnull StatisticsProvider statisticsProvider,
            @Nonnull String[] description,
            @Nonnull Closer closer
    ) throws RepositoryException {
        if (null == ds) {
            // Deferred registration - child class should call registerDataStore later
            return ds;
        }

        Closeables closeables = new Closeables(closer);

        boolean encodeLengthInId = PropertiesUtil.toBoolean(config.get(PROP_ENCODE_LENGTH), true);
        int cacheSizeInMB = PropertiesUtil.toInteger(config.get(PROP_CACHE_SIZE), DataStoreBlobStore.DEFAULT_CACHE_SIZE);

        String homeDir = lookup(context, PROP_HOME);
        if (config.containsKey(PATH) && !Strings.isNullOrEmpty((String) config.get(PATH))) {
            log.info("Initializing the DataStore with path [{}]", config.get(PATH));
            homeDir = (String) config.get(PATH);
        }
        else if (homeDir != null) {
            log.info("Initializing the DataStore with homeDir [{}]", homeDir);
        }
        PropertiesUtil.populate(ds, config, false);
        ds.init(homeDir);

        DataStoreBlobStore dataStore = new DataStoreBlobStore(ds, encodeLengthInId, cacheSizeInMB);
        closeables.add(dataStore);
        BlobStoreStats stats = new BlobStoreStats(statisticsProvider);
        dataStore.setBlobStatsCollector(stats);

        PropertiesUtil.populate(dataStore, config, false);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, ds.getClass().getName());
        props.put(DESCRIPTION, description);
        if (context.getProperties().get(PROP_SPLIT_BLOBSTORE) != null) {
            props.put(PROP_SPLIT_BLOBSTORE, context.getProperties().get(PROP_SPLIT_BLOBSTORE));
        }

        BundleContext bundleContext = context.getBundleContext();
        boolean alreadyRegistered = false;
        try {
            ServiceReference[] refs =bundleContext.getAllServiceReferences(BlobStore.class.getName(),
                    String.format("(%s=%s)", Constants.SERVICE_PID, ds.getClass().getName()));
            if (null != refs) {
                alreadyRegistered = true;
            }
        }
        catch (InvalidSyntaxException e) {
            log.warn("Couldn't check for preexisting data store service references", e);
        }
        if (! alreadyRegistered) {
            closeables.add(context.getBundleContext().registerService(new String[]{
                    BlobStore.class.getName(),
                    GarbageCollectableBlobStore.class.getName()
            }, dataStore, props));
        }

        closeables.add(registerMBeans(context.getBundleContext(), dataStore, stats));

        return ds;
    }

    protected String[] getDescription(){
        return new String[] {"type=unknown"};
    }

    void setStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    protected static String lookup(ComponentContext context, String property) {
        //Prefer property from BundleContext first
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property);
        }

        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        return null;
    }

    private static Registration registerMBeans(BundleContext context, DataStoreBlobStore ds, BlobStoreStats stats){
        Whiteboard wb = new OsgiWhiteboard(context);
        return new CompositeRegistration(
                registerMBean(wb,
                        BlobStoreStatsMBean.class,
                        stats,
                        BlobStoreStatsMBean.TYPE,
                        ds.getClass().getSimpleName()),
                registerMBean(wb,
                        CacheStatsMBean.class,
                        ds.getCacheStats(),
                        CacheStatsMBean.TYPE,
                        ds.getCacheStats().getName())
        );
    }
}

class Closeables implements Closeable {
    private final Closer closer;

    Closeables(final Closer closer) {
        this.closer = closer;
    }

    void add(Closeable c) {
        closer.register(c);
    }

    void add(DataStore ds) {
        add(new Closeable() {
            @Override public void close() throws IOException {
                try {
                    ds.close();
                }
                catch (DataStoreException e) {
                    throw new IOException(e);
                }
            }
        });
    }

    void add(ServiceRegistration sr) {
        add(new Closeable() {
            @Override public void close() throws IOException {
                sr.unregister();
            }
        });
    }

    void add(Registration r) {
        add(new Closeable() {
            @Override public void close() throws IOException {
                r.unregister();
            }
        });
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
