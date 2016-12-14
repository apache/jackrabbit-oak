/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.Closer;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory allowing creation of secondary segment node stores.
 * <p>
 * The different secondaries are distinguished by their role attribute.
 */
@Component(policy = ConfigurationPolicy.REQUIRE,
        name="org.apache.jackrabbit.oak.segment.SegmentNodeStoreFactory",
        configurationFactory=true,
        metatype = true,
        label = "Apache Jackrabbit Oak Segment-Tar NodeStore Factory",
        description = "Factory allowing configuration of adjacent instances of " +
                      "NodeStore implementation based on Segment model besides a default SegmentNodeStore in same setup."
)
public class SegmentNodeStoreFactory {

    @Property(
            label = "Role",
            description="As multiple SegmentNodeStores can be configured, this parameter defines the role " +
                        "of 'this' SegmentNodeStore."
    )
    public static final String ROLE = "role";

    @Property(boolValue = false,
            label = "Custom BlobStore",
            description = "Boolean value indicating that a custom BlobStore is to be used. " +
                    "By default large binary content would be stored within segment tar files"
    )
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    @Property(boolValue = false,
            label = "Register JCR descriptors as OSGi services",
            description="Should only be done for one factory instance")
    public static final String REGISTER_DESCRIPTORS = "registerDescriptors";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.STATIC,
            policyOption = ReferencePolicyOption.GREEDY,
            target = ONLY_STANDALONE_TARGET
    )
    private volatile BlobStore blobStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private Closer registrations = Closer.create();

    @Activate
    public void activate(ComponentContext context) throws IOException {
        String role = property(ROLE, context);
        // In secondaryNodeStore mode customBlobStore is always enabled
        boolean isSecondaryStoreMode = "secondary".equals(role);
        boolean customBlobStore = Boolean.parseBoolean(property(CUSTOM_BLOB_STORE, context)) || isSecondaryStoreMode;
        boolean registerRepositoryDescriptors = Boolean.parseBoolean(property(REGISTER_DESCRIPTORS, context));
        log.info("activate: SegmentNodeStore '" + role + "' starting.");

        if (blobStore == null && customBlobStore) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
            return;
        }

        if (role != null) {
            registrations = Closer.create();
            OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
            final SegmentNodeStore store = SegmentNodeStoreService.registerSegmentStore(context, blobStore,
                    statisticsProvider, registrations, whiteboard, role, registerRepositoryDescriptors);
            if (store != null) {
                Map<String, Object> props = new HashMap<String, Object>();
                props.put(NodeStoreProvider.ROLE, role);

                registrations
                        .register(asCloseable(whiteboard.register(NodeStoreProvider.class, new NodeStoreProvider() {

                            @Override
                            public NodeStore getNodeStore() {
                                return store;
                            }
                        }, props)));
                log.info("Registered NodeStoreProvider backed by SegmentNodeStore of type '{}'", role);
            }
        }
    }

    @Deactivate
    public void deactivate() {
        if (registrations != null) {
            IOUtils.closeQuietly(registrations);
            registrations = null;
        }
    }

    private static Closeable asCloseable(final Registration r) {
        return new Closeable() {

            @Override
            public void close() {
                r.unregister();
            }

        };
    }

    static String property(String name, ComponentContext context) {
        return lookupConfigurationThenFramework(context, name);
    }


}
