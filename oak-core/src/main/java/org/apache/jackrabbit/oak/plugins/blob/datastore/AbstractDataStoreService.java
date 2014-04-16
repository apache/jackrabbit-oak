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

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataStoreService {
    private static final String PROP_HOME = "repository.home";

    public static final String PROP_ENCODE_LENGTH = "encodeLengthInId";
    public static final String PROP_CACHE_SIZE = "cacheSizeInMB";

    private ServiceRegistration reg;

    private Logger log = LoggerFactory.getLogger(getClass());

    private DataStore dataStore;

    protected void activate(ComponentContext context, Map<String, Object> config) throws RepositoryException {
        DataStore ds = createDataStore(context, config);
        boolean encodeLengthInId = PropertiesUtil.toBoolean(config.get(PROP_ENCODE_LENGTH), true);
        int cacheSizeInMB = PropertiesUtil.toInteger(config.get(PROP_CACHE_SIZE), DataStoreBlobStore.DEFAULT_CACHE_SIZE);
        String homeDir = lookup(context, PROP_HOME);
        if (homeDir != null) {
            log.info("Initializing the DataStore with homeDir [{}]", homeDir);
        }
        PropertiesUtil.populate(ds, config, false);
        ds.init(homeDir);
        this.dataStore = new DataStoreBlobStore(ds, encodeLengthInId, cacheSizeInMB);
        PropertiesUtil.populate(dataStore, config, false);

        Dictionary<String, String> props = new Hashtable<String, String>();
        props.put(Constants.SERVICE_PID, ds.getClass().getName());

        reg = context.getBundleContext().registerService(new String[]{
                BlobStore.class.getName(),
                GarbageCollectableBlobStore.class.getName()
        }, dataStore , props);
    }

    protected void deactivate() throws DataStoreException {
        if (reg != null) {
            reg.unregister();
        }

        dataStore.close();
    }

    protected abstract DataStore createDataStore(ComponentContext context, Map<String, Object> config);

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
}
