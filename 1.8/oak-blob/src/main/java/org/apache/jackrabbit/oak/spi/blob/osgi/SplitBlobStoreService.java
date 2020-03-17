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

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.DefaultSplitBlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.WrappingSplitBlobStore;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.BlobStoreType.*;

@Component(policy = ConfigurationPolicy.REQUIRE)
public class SplitBlobStoreService {
    private static final Logger log = LoggerFactory.getLogger(SplitBlobStoreService.class);

    @Property
    private static final String PROP_HOME = "repository.home";

    @Property(options = { @PropertyOption(name = "External", value = "EXTERNAL"),
            @PropertyOption(name = "Internal - Segment", value = "SEGMENT"),
            @PropertyOption(name = "Internal - Document", value = "DOCUMENT") })
    private static final String PROP_OLD_BLOB_STORE_TYPE = "split.old.blobstore.type";

    public static final String PROP_SPLIT_BLOBSTORE = "split.blobstore";

    public static final String ONLY_STANDALONE_TARGET = "(&(!(split.blobstore=old))(!(split.blobstore=new)))";

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY, policy = ReferencePolicy.DYNAMIC, target = "(split.blobstore=old)")
    private BlobStore oldBlobStore;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY, policy = ReferencePolicy.DYNAMIC, target = "(split.blobstore=new)")
    private BlobStore newBlobStore;

    private BundleContext ctx;

    private ServiceRegistration reg;

    private String homeDir;

    private BlobStoreType oldBlobStoreType;

    @Activate
    protected void activate(ComponentContext context, Map<String, Object> config) throws InvalidSyntaxException {
        String oldTypeName = lookup(context, PROP_OLD_BLOB_STORE_TYPE);
        if (oldTypeName == null) {
            oldBlobStoreType = BlobStoreType.EXTERNAL;
        } else {
            oldBlobStoreType = BlobStoreType.valueOf(oldTypeName);
        }
        homeDir = lookup(context, PROP_HOME);
        if (homeDir != null) {
            log.info("Initializing the SplitBlobStore with home [{}]", homeDir);
        } else {
            log.warn("Can't initialize SplitBlobStore - empty {}", PROP_HOME);
            return;
        }
        ctx = context.getBundleContext();
        registerSplitBlobStore();
    }

    @Deactivate
    protected void deactivate() {
        unregisterSplitBlobStore();
        ctx = null;
    }

    private void registerSplitBlobStore() {
        if (oldBlobStore == null && oldBlobStoreType == BlobStoreType.EXTERNAL) {
            log.info("No BlobStore with ({}=old)", PROP_SPLIT_BLOBSTORE);
            return;
        }
        if (newBlobStore == null) {
            log.info("No BlobStore with ({}=new)", PROP_SPLIT_BLOBSTORE);
            return;
        }
        if (reg != null) {
            log.info("SplitBlobStore already registered");
            return;
        }
        if (ctx == null) {
            log.info("Component not activated yet");
            return;
        }
        log.info("Registering SplitBlobStore with old={} ({}) and new={}", oldBlobStore, oldBlobStoreType,
                newBlobStore);
        BlobStore blobStore;
        if (oldBlobStoreType == EXTERNAL || oldBlobStoreType == SEGMENT) {
            blobStore = new DefaultSplitBlobStore(homeDir, oldBlobStore, newBlobStore);
        } else if (oldBlobStoreType == DOCUMENT) {
            blobStore = new WrappingSplitBlobStore(homeDir, newBlobStore);
        } else {
            throw new IllegalStateException("Illegal blob store type value: " + oldBlobStoreType);
        }
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put("service.pid", "org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore");
        reg = ctx.registerService(new String[] { BlobStore.class.getName() }, blobStore, props);
    }

    private void unregisterSplitBlobStore() {
        if (reg != null) {
            reg.unregister();
        }
        reg = null;
    }

    private static String lookup(ComponentContext context, String property) {
        // Prefer property from BundleContext first
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property);
        }

        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        return null;
    }

    protected void bindOldBlobStore(BlobStore blobStore) {
        this.oldBlobStore = blobStore;
        registerSplitBlobStore();
    }

    protected void unbindOldBlobStore(BlobStore blobStore) {
        this.oldBlobStore = null;
        unregisterSplitBlobStore();
    }

    protected void bindNewBlobStore(BlobStore blobStore) {
        this.newBlobStore = blobStore;
        registerSplitBlobStore();
    }

    protected void unbindNewBlobStore(BlobStore blobStore) {
        this.newBlobStore = null;
        unregisterSplitBlobStore();
    }

    enum BlobStoreType {
        EXTERNAL, DOCUMENT, SEGMENT
    }
}
