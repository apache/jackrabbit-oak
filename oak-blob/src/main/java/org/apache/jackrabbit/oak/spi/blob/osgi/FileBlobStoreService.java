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

package org.apache.jackrabbit.oak.spi.blob.osgi;

import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.PROP_SPLIT_BLOBSTORE;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(policy = ConfigurationPolicy.REQUIRE, name = FileBlobStoreService.NAME)
public class FileBlobStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.spi.blob.FileBlobStore";

    private static final String PROP_HOME = "repository.home";

    private ServiceRegistration reg;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Activate
    protected void activate(ComponentContext context, Map<String, Object> config) {
        String homeDir = lookup(context, PROP_HOME);
        if (homeDir != null) {
            log.info("Initializing the FileBlobStore with homeDir [{}]", homeDir);
        }
        BlobStore blobStore = new FileBlobStore(FilenameUtils.concat(homeDir,"datastore"));
        PropertiesUtil.populate(blobStore, config, false);
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        if (context.getProperties().get(PROP_SPLIT_BLOBSTORE) != null) {
            props.put(PROP_SPLIT_BLOBSTORE, context.getProperties().get(PROP_SPLIT_BLOBSTORE));
        }
        reg = context.getBundleContext().registerService(new String[]{
                BlobStore.class.getName(),
                GarbageCollectableBlobStore.class.getName()
        }, blobStore, props);
    }

    @Deactivate
    protected void deactivate() {
        if (reg != null) {
            reg.unregister();
        }
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
}
