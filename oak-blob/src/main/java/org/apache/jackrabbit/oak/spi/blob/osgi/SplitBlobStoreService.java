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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(policy = ConfigurationPolicy.REQUIRE, name = SplitBlobStoreService.NAME)
public class SplitBlobStoreService {
    private static final Logger log = LoggerFactory.getLogger(SplitBlobStoreService.class);

    public static final String NAME = "org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore";

    private static final String PROP_HOME = "repository.home";

    public static final String PROP_SPLIT_BLOBSTORE = "split.blobstore";

    public static final String ONLY_STANDALONE_TARGET = "(&(!(split.blobstore=old))(!(split.blobstore=new)))";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, target = "(split.blobstore=old)")
    private BlobStore oldBlobStore;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, target = "(split.blobstore=new)")
    private BlobStore newBlobStore;

    private ServiceRegistration reg;

    @Activate
    protected void activate(ComponentContext context, Map<String, Object> config) throws InvalidSyntaxException {
        final String homeDir = lookup(context, PROP_HOME);
        if (homeDir != null) {
            log.info("Initializing the SplitBlobStore with old [{}], new [{}] and home [{}]", oldBlobStore.getClass(),
                    newBlobStore.getClass(), homeDir);
        } else {
            log.warn("Can't initialize SplitBlobStore - empty {}", PROP_HOME);
            return;
        }
        final BlobStore blobStore = new SplitBlobStore(homeDir, oldBlobStore, newBlobStore);
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put("service.pid", "org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore");
        reg = context.getBundleContext().registerService(new String[] { BlobStore.class.getName() }, blobStore, props);
    }

    @Deactivate
    protected void deactivate() {
        if (reg != null) {
            reg.unregister();
        }
    }

    protected static String lookup(ComponentContext context, String property) {
        // Prefer property from BundleContext first
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property);
        }

        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        return null;
    }
}
