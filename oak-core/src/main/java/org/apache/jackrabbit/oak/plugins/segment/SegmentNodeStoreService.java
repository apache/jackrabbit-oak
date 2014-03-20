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
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.commons.io.FilenameUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ProxyNodeStore;
import org.apache.jackrabbit.oak.spi.state.RevisionGC;
import org.apache.jackrabbit.oak.spi.state.RevisionGCMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(policy = ConfigurationPolicy.REQUIRE)
public class SegmentNodeStoreService extends ProxyNodeStore
        implements Observable {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="TarMK directory")
    public static final String DIRECTORY = "repository.home";

    @Property(description="TarMK mode (64 for memory mapping, 32 for normal file access)")
    public static final String MODE = "tarmk.mode";

    @Property(description="TarMK maximum file size (MB)", intValue=256)
    public static final String SIZE = "tarmk.size";

    @Property(description="Cache size (MB)", intValue=256)
    public static final String CACHE = "cache";

    /**
     * Boolean value indicating a blobStore is to be used
     */
    public static final String CUSTOM_BLOB_STORE = "customBlobStore";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String name;

    private SegmentStore store;

    private SegmentNodeStore delegate;

    private ObserverTracker observerTracker;

    private ComponentContext context;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
    private volatile BlobStore blobStore;

    private ServiceRegistration registration;
    private Registration mBeanRegistration;
    private WhiteboardExecutor executor;

    @Override
    protected synchronized SegmentNodeStore getNodeStore() {
        checkState(delegate != null, "service must be activated when used");
        return delegate;
    }

    @Activate
    private void activate(ComponentContext context) throws IOException {
        this.context = context;

        if(blobStore == null &&
                Boolean.parseBoolean(lookup(context, CUSTOM_BLOB_STORE))){
            log.info("BlobStore use enabled. SegmentNodeStore would be initialized when BlobStore would be available");
        }else{
            registerNodeStore();
        }
    }

    public synchronized void registerNodeStore()
            throws IOException {
        if(context == null){
            log.info("Component still not activated. Ignoring the initialization call");
            return;
        }

        Dictionary<?, ?> properties = context.getProperties();
        name = "" + properties.get(NAME);

        String directory = lookup(context, DIRECTORY);
        if (directory == null) {
            directory = "tarmk";
        }else{
            directory = FilenameUtils.concat(directory, "segmentstore");
        }

        String mode = lookup(context, MODE);
        if (mode == null) {
            mode = System.getProperty(MODE,
                    System.getProperty("sun.arch.data.model", "32"));
        }

        String size = lookup(context, SIZE);
        if (size == null) {
            size = System.getProperty(SIZE, "256");
        }

        store = new FileStore(
                blobStore,
                new File(directory),
                Integer.parseInt(size), "64".equals(mode));

        delegate = new SegmentNodeStore(store);
        observerTracker = new ObserverTracker(delegate);
        observerTracker.start(context.getBundleContext());

        Dictionary<String, String> props = new Hashtable<String, String>();
        props.put(Constants.SERVICE_PID, SegmentNodeStore.class.getName());
        registration = context.getBundleContext().registerService(NodeStore.class.getName(), this, props);

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        RevisionGC revisionGC = new RevisionGC(new Runnable() {
            @Override
            public void run() {
                store.gc();
            }
        }, executor);
        mBeanRegistration = registerMBean(whiteboard, RevisionGCMBean.class, revisionGC,
                RevisionGCMBean.TYPE, "Segment node store revision garbage collection");

        log.info("SegmentNodeStore initialized");
    }

    private static String lookup(ComponentContext context, String property) {
        if (context.getProperties().get(property) != null) {
            return context.getProperties().get(property).toString();
        }
        if (context.getBundleContext().getProperty(property) != null) {
            return context.getBundleContext().getProperty(property);
        }
        return null;
    }

    @Deactivate
    public synchronized void deactivate() {
        unregisterNodeStore();

        observerTracker.stop();
        delegate = null;

        store.close();
        store = null;
    }

    protected void bindBlobStore(BlobStore blobStore) throws IOException {
        log.info("Initializing SegmentNodeStore with BlobStore [{}]", blobStore);
        this.blobStore = blobStore;
        registerNodeStore();
    }

    protected void unbindBlobStore(BlobStore blobStore){
        this.blobStore = null;
        unregisterNodeStore();
    }

    private void unregisterNodeStore() {
        if(registration != null){
            registration.unregister();
            registration = null;
        }
        if (mBeanRegistration != null) {
            mBeanRegistration.unregister();
            mBeanRegistration = null;
        }
        if (executor != null) {
            executor.stop();
            executor = null;
        }
    }

    /**
     * needed for situations where you have to unwrap the
     * SegmentNodeStoreService, to get the SegmentStore, like the failover
     */
    public SegmentStore getSegmentStore() {
        return store;
    }

    //------------------------------------------------------------< Observable >---

    @Override
    public Closeable addObserver(Observer observer) {
        return getNodeStore().addObserver(observer);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return name + ": " + delegate;
    }
}
