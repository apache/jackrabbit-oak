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

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Designate(ocd = SecondaryStoreCacheService.Configuration.class)
public class SecondaryStoreCacheService {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak DocumentNodeStateCache Provider",
            description = "Configures a DocumentNodeStateCache based on a secondary NodeStore"
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "Included Paths",
                description = "List of paths which are to be included in the secondary store"
        )
        String[] includedPaths() default {"/"};

        @AttributeDefinition(
                name = "Async Observation",
                description = "Enable async observation processing"
        )
        boolean enableAsyncObserver() default true;

        @AttributeDefinition(
                name = "Observer queue size",
                description = "Observer queue size. Used if 'enableAsyncObserver' is set to true"
        )
        int observerQueueSize() default BackgroundObserver.DEFAULT_QUEUE_SIZE;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * Having a reference to BlobStore ensures that DocumentNodeStoreService does register a BlobStore
     */
    @Reference
    private BlobStore blobStore;

    @Reference(target = "(role=secondary)")
    private NodeStoreProvider secondaryStoreProvider;

    @Reference
    private Executor executor;

    @Reference
    private StatisticsProvider statisticsProvider;

    /*
     * Have an optional dependency on DocumentNodeStore such that we do not have hard dependency
     * on it and DocumentNodeStore can make use of this service even after it has unregistered
     */
    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC)
    private volatile DocumentNodeStore documentNodeStore;

    private final List<Registration> oakRegs = Lists.newArrayList();

    private final List<ServiceRegistration> regs = Lists.newArrayList();

    private Whiteboard whiteboard;

    private BundleContext bundleContext;

    private PathFilter pathFilter;

    private final MultiplexingNodeStateDiffer differ = new MultiplexingNodeStateDiffer();

    @Activate
    private void activate(BundleContext context, Configuration config){
        bundleContext = context;
        whiteboard = new OsgiWhiteboard(context);
        String[] includedPaths = config.includedPaths();

        //TODO Support for exclude is not possible as once a NodeState is loaded from secondary
        //store it assumes that complete subtree is in same store. With exclude it would need to
        //check for each child access and route to primary
        pathFilter = new PathFilter(asList(includedPaths), Collections.<String>emptyList());

        SecondaryStoreBuilder builder = new SecondaryStoreBuilder(secondaryStoreProvider.getNodeStore())
                .differ(differ)
                .metaPropNames(DocumentNodeStore.META_PROP_NAMES)
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

    private void registerObserver(Observer observer, Configuration config) {
        boolean enableAsyncObserver = config.enableAsyncObserver();
        int  queueSize = config.observerQueueSize();
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
