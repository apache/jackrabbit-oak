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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toStringArray;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(label = "Apache Jackrabbit Oak DocumentNodeStateCache Provider",
        metatype = true,
        policy = ConfigurationPolicy.REQUIRE,
        description = "Configures a DocumentNodeStateCache based on a secondary NodeStore"
)
public class SecondaryStoreCacheService {
    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * Having a reference to BlobStore ensures that DocumentNodeStoreService does register a BlobStore
     */
    @Reference
    private BlobStore blobStore;

    @Reference(target = "(type=secondary)")
    private NodeStore secondaryStore;

    @Reference
    private Executor executor;

    @Reference
    private StatisticsProvider statisticsProvider;

    /*
     * Have an optional dependency on DocumentNodeStore such that we do not have hard dependency
     * on it and DocumentNodeStore can make use of this service even after it has unregistered
     */
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC)
    private DocumentNodeStore documentNodeStore;

    @Property(unbounded = PropertyUnbounded.ARRAY,
            label = "Included Paths",
            description = "List of paths which are to be included in the secondary store",
            value = {"/"}
    )
    private static final String PROP_INCLUDES = "includedPaths";

    @Property(unbounded = PropertyUnbounded.ARRAY,
            label = "Excluded Paths",
            description = "List of paths which are to be excluded in the secondary store",
            value = {}
    )
    private static final String PROP_EXCLUDES = "excludedPaths";


    private static final boolean PROP_ASYNC_OBSERVER_DEFAULT = true;
    @Property(
            boolValue = PROP_ASYNC_OBSERVER_DEFAULT,
            label = "Async Observation",
            description = "Enable async observation processing"
    )
    private static final String PROP_ASYNC_OBSERVER = "enableAsyncObserver";

    private static final int PROP_OBSERVER_QUEUE_SIZE_DEFAULT = 1000;
    @Property(
            intValue = PROP_OBSERVER_QUEUE_SIZE_DEFAULT,
            label = "Observer queue size",
            description = "Observer queue size. Used if 'enableAsyncObserver' is set to true"
    )
    private static final String PROP_OBSERVER_QUEUE_SIZE = "observerQueueSize";

    private final List<Registration> oakRegs = Lists.newArrayList();

    private final List<ServiceRegistration> regs = Lists.newArrayList();

    private Whiteboard whiteboard;

    private BundleContext bundleContext;

    private PathFilter pathFilter;

    private final MultiplexingNodeStateDiffer differ = new MultiplexingNodeStateDiffer();

    @Activate
    private void activate(BundleContext context, Map<String, Object> config){
        bundleContext = context;
        whiteboard = new OsgiWhiteboard(context);
        String[] includedPaths = toStringArray(config.get(PROP_INCLUDES), new String[]{"/"});
        String[] excludedPaths = toStringArray(config.get(PROP_EXCLUDES), new String[]{""});

        pathFilter = new PathFilter(asList(includedPaths), asList(excludedPaths));

        SecondaryStoreBuilder builder = new SecondaryStoreBuilder(secondaryStore)
                .differ(differ)
                .statisticsProvider(statisticsProvider)
                .pathFilter(pathFilter);
        SecondaryStoreCache cache = builder.buildCache();
        SecondaryStoreObserver observer = builder.buildObserver(cache);
        registerObserver(observer, config);

        regs.add(bundleContext.registerService(DocumentNodeStateCache.class.getName(), cache, null));

        //TODO Need to see OSGi dynamics. Its possible that DocumentNodeStore works after the cache
        //gets deregistered but the SegmentNodeState instances might still be in use and that would cause
        //failure
    }

    @Deactivate
    private void deactivate(){
        for (Registration r : oakRegs){
            r.unregister();
        }
        for (ServiceRegistration r : regs){
            r.unregister();
        }
    }

    PathFilter getPathFilter() {
        return pathFilter;
    }

    protected void bindDocumentNodeStore(DocumentNodeStore documentNodeStore){
        log.info("Registering DocumentNodeStore as the differ");
        differ.setDelegate(documentNodeStore);
    }

    protected void unbindDocumentNodeStore(DocumentNodeStore documentNodeStore){
        differ.setDelegate(NodeStateDiffer.DEFAULT_DIFFER);
    }

    //~----------------------------------------------------< internal >

    private void registerObserver(Observer observer, Map<String, Object> config) {
        boolean enableAsyncObserver = toBoolean(config.get(PROP_ASYNC_OBSERVER), PROP_ASYNC_OBSERVER_DEFAULT);
        int  queueSize = toInteger(config.get(PROP_OBSERVER_QUEUE_SIZE), PROP_OBSERVER_QUEUE_SIZE_DEFAULT);
        if (enableAsyncObserver){
            BackgroundObserver bgObserver = new BackgroundObserver(observer, executor, queueSize);
            oakRegs.add(registerMBean(whiteboard,
                    BackgroundObserverMBean.class,
                    bgObserver.getMBean(),
                    BackgroundObserverMBean.TYPE,
                    "Secondary NodeStore observer stats"));
            observer = bgObserver;
            log.info("Configuring the observer for secondary NodeStore as " +
                    "Background Observer with queue size {}", queueSize);
        }

        //Ensure that our observer comes first in processing
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(Constants.SERVICE_RANKING, 10000);
        regs.add(bundleContext.registerService(Observer.class.getName(), observer, props));
    }

    private static class MultiplexingNodeStateDiffer implements NodeStateDiffer {
        private volatile NodeStateDiffer delegate = NodeStateDiffer.DEFAULT_DIFFER;
        @Override
        public boolean compare(@Nonnull AbstractDocumentNodeState node,
                               @Nonnull AbstractDocumentNodeState base, @Nonnull NodeStateDiff diff) {
            return delegate.compare(node, base, diff);
        }

        public void setDelegate(NodeStateDiffer delegate) {
            this.delegate = delegate;
        }
    }
}
