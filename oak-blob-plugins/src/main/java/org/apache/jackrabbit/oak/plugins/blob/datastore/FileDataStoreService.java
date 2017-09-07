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

import com.google.common.base.Preconditions;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

@Component(policy = ConfigurationPolicy.REQUIRE, name = FileDataStoreService.NAME)
public class FileDataStoreService extends AbstractDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore";

    private static final String DESCRIPTION = "oak.datastore.description";

    public static final String CACHE_PATH = "cachePath";
    public static final String CACHE_SIZE = "cacheSize";
    public static final String FS_BACKEND_PATH = "fsBackendPath";
    public static final String PATH = "path";

    private ServiceRegistration delegateReg;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {

        long cacheSize = PropertiesUtil.toLong(config.get(CACHE_SIZE), 0L);
        // return CachingFDS when cacheSize > 0
        if (cacheSize > 0) {
            String fsBackendPath = PropertiesUtil.toString(config.get(PATH), null);
            Preconditions.checkNotNull(fsBackendPath, "Cannot create " +
                    "FileDataStoreService with caching. [{path}] property not configured.");

            config.remove(PATH);
            config.remove(CACHE_SIZE);
            config.put(FS_BACKEND_PATH, fsBackendPath);
            config.put("cacheSize", cacheSize);
            String cachePath = PropertiesUtil.toString(config.get(CACHE_PATH), null);
            if (cachePath != null) {
                config.remove(CACHE_PATH);
                config.put(PATH, cachePath);
            }
            Properties properties = new Properties();
            properties.putAll(config);
            log.info("Initializing with properties " + properties);

            return getCachingDataStore(properties, context);
        } else {
            log.info("OakFileDataStore initialized");
            return new OakFileDataStore();
        }
    }

    private DataStore getCachingDataStore(Properties props, ComponentContext context) {
        CachingFileDataStore dataStore = new CachingFileDataStore();
        dataStore.setStagingSplitPercentage(
            PropertiesUtil.toInteger(props.get("stagingSplitPercentage"), 0));
        dataStore.setProperties(props);
        Dictionary<String, Object> config = new Hashtable<String, Object>();
        config.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        config.put(DESCRIPTION, getDescription());

        delegateReg = context.getBundleContext().registerService(new String[] {
            AbstractSharedCachingDataStore.class.getName(),
            AbstractSharedCachingDataStore.class.getName()
        }, dataStore , config);
        return dataStore;
    }

    @Override
    protected String[] getDescription() {
        return new String[]{"type=filesystem"};
    }
}
