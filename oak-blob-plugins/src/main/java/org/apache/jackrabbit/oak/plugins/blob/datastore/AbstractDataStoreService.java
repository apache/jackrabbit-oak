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

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.PROP_SPLIT_BLOBSTORE;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(componentAbstract = true)
public abstract class AbstractDataStoreService {
    private static final String PROP_HOME = "repository.home";

    public static final String PROP_ENCODE_LENGTH = "encodeLengthInId";
    public static final String PROP_CACHE_SIZE = "cacheSizeInMB";
    private static final String DESCRIPTION = "oak.blobstore.description";

    private ServiceRegistration reg;

    private Registration mbeanReg;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Reference
    private StatisticsProvider statisticsProvider;

    private DataStoreBlobStore dataStore;

    protected void activate(ComponentContext context, Map<String, Object> config) throws RepositoryException {
        // change to mutable map. may be modified in createDS call
        config = Maps.newHashMap(config);
        DataStore ds = createDataStore(context, config);
        boolean encodeLengthInId = PropertiesUtil.toBoolean(config.get(PROP_ENCODE_LENGTH), true);
        int cacheSizeInMB = PropertiesUtil.toInteger(config.get(PROP_CACHE_SIZE), DataStoreBlobStore.DEFAULT_CACHE_SIZE);
        String homeDir = lookup(context, PROP_HOME);
        if (homeDir != null) {
            log.info("Initializing the DataStore with homeDir [{}]", homeDir);
        }
        PropertiesUtil.populate(ds, config, false);
        ds.init(homeDir);
        BlobStoreStats stats = new BlobStoreStats(getStatisticsProvider());
        this.dataStore = new DataStoreBlobStore(ds, encodeLengthInId, cacheSizeInMB);
        this.dataStore.setBlobStatsCollector(stats);
        PropertiesUtil.populate(dataStore, config, false);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, ds.getClass().getName());
        props.put(DESCRIPTION, getDescription());
        if (context.getProperties().get(PROP_SPLIT_BLOBSTORE) != null) {
            props.put(PROP_SPLIT_BLOBSTORE, context.getProperties().get(PROP_SPLIT_BLOBSTORE));
        }

        reg = context.getBundleContext().registerService(new String[]{
                BlobStore.class.getName(),
                GarbageCollectableBlobStore.class.getName()
        }, dataStore , props);

        mbeanReg = registerMBeans(context.getBundleContext(), dataStore, stats);
    }

    protected void deactivate() throws DataStoreException {
        if (reg != null) {
            reg.unregister();
        }

        if (mbeanReg != null){
            mbeanReg.unregister();
        }

        dataStore.close();
    }

    protected abstract DataStore createDataStore(ComponentContext context, Map<String, Object> config);

    protected StatisticsProvider getStatisticsProvider(){
        return statisticsProvider;
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
