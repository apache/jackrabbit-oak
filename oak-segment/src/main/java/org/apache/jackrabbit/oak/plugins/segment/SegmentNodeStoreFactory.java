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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.Builder;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStoreStatsMBean;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory allowing creation of secondary segment node stores.
 * <p>
 * The different secondaries are distinguished by their role attribute.
 */
@Component(policy = ConfigurationPolicy.REQUIRE,
        name="org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStoreFactory",
        configurationFactory=true,
        metatype = true,
        label = "Apache Jackrabbit Oak Segment NodeStore Factory",
        description = "Factory allowing configuration of adjacent instances of " +
                      "NodeStore implementation based on Segment model besides a default SegmentNodeStore in same setup."
)
@Deprecated
public class SegmentNodeStoreFactory extends ProxyNodeStore {

    @Deprecated
    public static final String NAME = "name";

    @Property(
            label = "Role",
            description="As multiple SegmentNodeStores can be configured, this parameter defines the role " +
                        "of 'this' SegmentNodeStore."
    )
    @Deprecated
    public static final String ROLE = "role";

    @Property(
            label = "Directory",
            description="Directory location used to store the segment tar files. If not specified then looks " +
                    "for framework property 'repository.home' otherwise use a subdirectory with name 'tarmk'"
    )
    @Deprecated
    public static final String DIRECTORY = "repository.home";

    @Property(
            label = "Mode",
            description="TarMK mode (64 for memory mapping, 32 for normal file access)"
    )
    @Deprecated
    public static final String MODE = "tarmk.mode";

    @Property(
            intValue = 256,
            label = "Maximum Tar File Size (MB)",
            description = "TarMK maximum file size (MB)"
    )
    @Deprecated
    public static final String SIZE = "tarmk.size";

    @Property(
            intValue = 256,
            label = "Cache size (MB)",
            description = "Cache size for storing most recently used Segments"
    )
    @Deprecated
    public static final String CACHE = "cache";

    @Property(boolValue = false,
            label = "Custom BlobStore",
            description = "Boolean value indicating that a custom BlobStore is to be used. " +
                    "By default large binary content would be stored within segment tar files"
    )
    @Deprecated
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String name;

    private FileStore store;

    private volatile SegmentNodeStore segmentNodeStore;

    private ComponentContext context;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC, target = ONLY_STANDALONE_TARGET)
    private volatile BlobStore blobStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private ServiceRegistration storeRegistration;
    private Registration fileStoreStatsMBean;
    private WhiteboardExecutor executor;

    private boolean customBlobStore;

    private String role;

    @Override
    protected SegmentNodeStore getNodeStore() {
        checkState(segmentNodeStore != null, "service must be activated when used");
        return segmentNodeStore;
    }

    @Activate
    @Deprecated
    public void activate(ComponentContext context) throws IOException {
        this.context = context;
        this.name = PropertiesUtil.toString(context.getProperties().get(NAME), "SegmentNodeStore instance");
        this.role = property(ROLE);
        //In secondaryNodeStore mode customBlobStore is always enabled
        this.customBlobStore = Boolean.parseBoolean(property(CUSTOM_BLOB_STORE)) || isSecondaryStoreMode();
        log.info("activate: SegmentNodeStore '"+role+"' starting.");

        if (blobStore == null && customBlobStore) {
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
        } else {
            registerNodeStore();
        }
    }

    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        this.blobStore = blobStore;
        registerNodeStore();
    }

    protected void unbindBlobStore(BlobStore blobStore){
        this.blobStore = null;
        unregisterNodeStore();
    }

    @Deactivate
    @Deprecated
    public void deactivate() {
        unregisterNodeStore();

        synchronized (this) {
            segmentNodeStore = null;

            if (store != null) {
                store.close();
                store = null;
            }
        }
    }

    private synchronized void registerNodeStore() throws IOException {
        if (registerSegmentStore() && role != null) {
            registerNodeStoreProvider();
        }
    }

    private boolean isSecondaryStoreMode() {
        return "secondary".equals(role);
    }

    private void registerNodeStoreProvider() {
        SegmentNodeStore.SegmentNodeStoreBuilder nodeStoreBuilder = SegmentNodeStore.builder(store);
        segmentNodeStore = nodeStoreBuilder.build();
        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(NodeStoreProvider.ROLE, role);
        storeRegistration = context.getBundleContext().registerService(NodeStoreProvider.class.getName(), new NodeStoreProvider() {
                    @Override
                    public NodeStore getNodeStore() {
                        return SegmentNodeStoreFactory.this;
                    }
                },
                props);
        log.info("Registered NodeStoreProvider backed by SegmentNodeStore of type '{}'", role);
    }

    private boolean registerSegmentStore() throws IOException {
        if (context == null) {
            log.info("Component still not activated. Ignoring the initialization call");
            return false;
        }

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        // Build the FileStore

        Builder builder = FileStore.builder(getDirectory())
                .withCacheSize(getCacheSize())
                .withMaxFileSize(getMaxFileSize())
                .withMemoryMapping(getMode().equals("64"))
                .withStatisticsProvider(statisticsProvider);

        if (customBlobStore) {
            log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
            builder.withBlobStore(blobStore);
        }

        try {
            store = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            log.error("The segment store data is not compatible with the current version. Please use oak-segment-tar or a different version of oak-segment.");
            return false;
        }

        // Listen for Executor services on the whiteboard

        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        // Expose statistics about the FileStore

        fileStoreStatsMBean = registerMBean(
                whiteboard,
                FileStoreStatsMBean.class,
                store.getStats(),
                FileStoreStatsMBean.TYPE,
                "FileStore '" + role + "' statistics"
        );

        return true;
    }

    private void unregisterNodeStore() {
        if (storeRegistration != null) {
            storeRegistration.unregister();
            storeRegistration = null;
        }
        if (fileStoreStatsMBean != null) {
            fileStoreStatsMBean.unregister();
            fileStoreStatsMBean = null;
        }
        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    private File getBaseDirectory() {
        String directory = property(DIRECTORY);

        if (directory != null) {
            return new File(directory);
        }

        return new File("tarmk");
    }

    private File getDirectory() {
        String dirName = "segmentstore";
        if (role != null){
            dirName = role + "-" + dirName;
        }
        return new File(getBaseDirectory(), dirName);
    }

    private String getMode() {
        String mode = property(MODE);

        if (mode != null) {
            return mode;
        }

        return System.getProperty(MODE, System.getProperty("sun.arch.data.model", "32"));
    }

    private String getCacheSizeProperty() {
        String cache = property(CACHE);

        if (cache != null) {
            return cache;
        }

        return System.getProperty(CACHE);
    }

    private int getCacheSize() {
        return Integer.parseInt(getCacheSizeProperty());
    }

    private String getMaxFileSizeProperty() {
        String size = property(SIZE);

        if (size != null) {
            return size;
        }

        return System.getProperty(SIZE, "256");
    }

    private int getMaxFileSize() {
        return Integer.parseInt(getMaxFileSizeProperty());
    }

    private String property(String name) {
        return lookupConfigurationThenFramework(context, name);
    }

    //------------------------------------------------------------< Object >--

    @Override
    @Deprecated
    public String toString() {
        return name + ": " + segmentNodeStore + "[role:" + role + "]";
    }
}