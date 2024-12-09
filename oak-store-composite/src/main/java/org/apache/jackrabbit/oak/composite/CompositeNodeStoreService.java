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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ComponentPropertyType;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecks;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
public class CompositeNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeStoreService.class);

    private static final String GLOBAL_ROLE = "composite-global";

    private static final String MOUNT_ROLE_PREFIX = "composite-mount-";

    private MountInfoProvider mountInfoProvider;

    private List<NodeStoreWithProps> nodeStores = new ArrayList<>();
    
    private NodeStoreChecks checks;

    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private static final String ENABLE_CHECKS = "enableChecks";
    private static final String PROP_SEED_MOUNT = "seedMount";
    private static final String PATH_STATS = "pathStats";

    @ComponentPropertyType
    @interface Config {
        @AttributeDefinition(
                name = "Enable node store checks",
                description = "Whether the composite node store constraints should be checked before start"
        )
        boolean enableChecks() default true;

        @AttributeDefinition(
                name = "Pre-populate seed mount",
                description = "Setting this parameter to a mount name will enable pre-populating the empty default store"
        )
        String seedMount();

        @AttributeDefinition(
                name = "Gather path statistics",
                description = "Whether the CompositeNodeStoreStatsMBean should gather information about the most popular paths (may be expensive)"
        )
        boolean pathStats() default false;
    }

    private ComponentContext context;

    private final Set<NodeStoreProvider> nodeStoresInUse = CollectionUtils.newIdentityHashSet();

    private ServiceRegistration nsReg;

    private Closer mbeanRegistrations;

    private ObserverTracker observerTracker;

    private String seedMount;

    private boolean pathStats;

    private boolean enableChecks;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) throws IOException, CommitFailedException {
        this.context = context;
        seedMount = PropertiesUtil.toString(config.get(PROP_SEED_MOUNT), null);
        pathStats = PropertiesUtil.toBoolean(config.get(PATH_STATS), false);
        enableChecks = PropertiesUtil.toBoolean(config.get(ENABLE_CHECKS), true);
        registerCompositeNodeStore();
    }

    @Deactivate
    protected void deactivate() throws IOException {
        unregisterCompositeNodeStore();
    }

    private void registerCompositeNodeStore() throws IOException, CommitFailedException {
        if (nsReg != null) {
            return; // already registered
        }

        NodeStoreWithProps globalNs = nodeStores.stream().filter(this::isGlobalNodeStore).findFirst().orElse(null);

        if (globalNs == null) {
            LOG.info("Composite node store registration is deferred until there's a global node store registered in OSGi");
            return;
        } else {
            LOG.info("Found global node store: {}", globalNs.getDescription());
        }

        if (!allMountsAvailable(mountInfoProvider)) {
            return;
        }
        LOG.info("Node stores for all configured mounts are available");

        CompositeNodeStore.Builder builder = new CompositeNodeStore.Builder(mountInfoProvider, globalNs.getNodeStoreProvider().getNodeStore());
        nodeStoresInUse.add(globalNs.getNodeStoreProvider());

        if (enableChecks) {
            builder.with(checks);
        }

        for (NodeStoreWithProps ns : nodeStores) {
            if (isGlobalNodeStore(ns)) {
                continue;
            }
            String mountName = getMountName(ns);
            if (mountName == null) {
                continue;
            }

            builder.addMount(mountName, ns.getNodeStoreProvider().getNodeStore());
            LOG.info("Mounting {} as {}", ns.getDescription(), mountName);
            nodeStoresInUse.add(ns.getNodeStoreProvider());

            if (mountName.equals(seedMount)) {
                new InitialContentMigrator(globalNs.nodeStore.getNodeStore(), ns.getNodeStoreProvider().getNodeStore(), mountInfoProvider.getMountByName(seedMount)).migrate();
            }
        }

        CompositeNodeStoreStats nodeStateStats = new CompositeNodeStoreStats(statisticsProvider, "NODE_STATE", pathStats);
        CompositeNodeStoreStats nodeBuilderStats = new CompositeNodeStoreStats(statisticsProvider, "NODE_BUILDER", pathStats);
        builder.with(nodeStateStats, nodeBuilderStats);

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(Constants.SERVICE_PID, CompositeNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[] { "nodeStoreType=compositeStore" } );

        CompositeNodeStore store = builder.build();

        observerTracker = new ObserverTracker(store);
        observerTracker.start(context.getBundleContext());

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

        mbeanRegistrations = Closer.create();
        registerMBean(whiteboard,
                CheckpointMBean.class,
                new CompositeCheckpointMBean(store),
                CheckpointMBean.TYPE,
                "Composite node store checkpoint management");
        registerMBean(whiteboard,
                CompositeNodeStoreStatsMBean.class,
                nodeStateStats,
                CompositeNodeStoreStatsMBean.TYPE,
                "Composite node store statistics (node state)");
        registerMBean(whiteboard,
                CompositeNodeStoreStatsMBean.class,
                nodeBuilderStats,
                CompositeNodeStoreStatsMBean.TYPE,
                "Composite node store statistics (node builder)");

        LOG.info("Registering the composite node store");
        nsReg = context.getBundleContext().registerService(
                NodeStore.class.getName(),
                store,
                props);
    }

    private boolean allMountsAvailable(MountInfoProvider mountInfoProvider) {
        Set<String> availableMounts = nodeStores.stream()
            .map(this::getMountName)
            .filter(Objects::nonNull)
            .collect(toSet());

        for (Mount mount : mountInfoProvider.getNonDefaultMounts()) {
            if (!availableMounts.contains(mount.getName())) {
                LOG.info("Composite node store registration is deferred until there's mount {} registered in OSGi", mount.getName());
                return false;
            }
        }

        return true;
    }

    private <T> void registerMBean(Whiteboard whiteboard,
                          Class<T> iface, T bean, String type, String name) {
        Registration reg = WhiteboardUtils.registerMBean(whiteboard, iface, bean, type, name);
        mbeanRegistrations.register(reg::unregister);
    }

    private boolean isGlobalNodeStore(NodeStoreWithProps ns) {
        return GLOBAL_ROLE.equals(ns.getRole());
    }

    private String getMountName(NodeStoreWithProps ns) {
        String role = ns.getRole();
        if (role == null || !role.startsWith(MOUNT_ROLE_PREFIX)) {
            return null;
        }
        return role.substring(MOUNT_ROLE_PREFIX.length());
    }

    private void unregisterCompositeNodeStore() throws IOException {
        if (nsReg != null) {
            LOG.info("Unregistering the composite node store");
            nsReg.unregister();
            nsReg = null;
        }
        if (mbeanRegistrations != null) {
            mbeanRegistrations.close();
            mbeanRegistrations = null;
        }
        if (observerTracker != null) {
            observerTracker.stop();
            observerTracker = null;
        }
        nodeStoresInUse.clear();
    }

    @SuppressWarnings("unused")
    @Reference(name = "nodeStores",
            cardinality = ReferenceCardinality.AT_LEAST_ONE,
            policy = ReferencePolicy.DYNAMIC,
            service = NodeStoreProvider.class,
            target="(!(service.pid=org.apache.jackrabbit.oak.composite.CompositeNodeStore))"
            )
    protected void bindNodeStore(NodeStoreProvider ns, Map<String, ?> config) throws IOException, CommitFailedException {
        NodeStoreWithProps newNs = new NodeStoreWithProps(ns, config);
        nodeStores.add(newNs);

        if (context == null) {
            LOG.info("bindNodeStore: context is null, delaying reconfiguration");
            return;
        }

        if (nsReg == null) {
            registerCompositeNodeStore();
        }
    }

    @SuppressWarnings("unused")
    protected void unbindNodeStore(NodeStoreProvider ns) throws IOException {
        nodeStores.removeIf(nodeStoreWithProps -> nodeStoreWithProps.getNodeStoreProvider() == ns);

        if (context == null) {
            LOG.info("unbindNodeStore: context is null, delaying reconfiguration");
            return;
        }

        if (nsReg != null && nodeStoresInUse.contains(ns)) {
            unregisterCompositeNodeStore();
        }
    }

    @SuppressWarnings("unused")
    @Reference(name = "mountInfoProvider",
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC,
            service = MountInfoProvider.class
            )
    protected void bindMountInfoProvider(MountInfoProvider mip) {
        this.mountInfoProvider = mip;
    }

    @SuppressWarnings("unused")
    protected void unbindMountInfoProvider(MountInfoProvider mip) {
        if (this.mountInfoProvider == mip) {
            this.mountInfoProvider = null;
        }
    }

    @SuppressWarnings("unused")
    @Reference(name = "checks", service = NodeStoreChecks.class)
    protected void bindChecks(NodeStoreChecks checks) {
        this.checks = checks;
    }

    @SuppressWarnings("unused")
    protected void unbindChecks(NodeStoreChecks checks) {
        if (this.checks == checks) {
            this.checks = null;
        }
    }

    @SuppressWarnings("unused")
    @Reference(name = "statisticsProvider", service = StatisticsProvider.class)
    protected void bindStatisticsProvider(StatisticsProvider sp) {
        this.statisticsProvider = sp;
    }

    @SuppressWarnings("unused")
    protected void unbindStatisticsProvider(StatisticsProvider sp) {
        if (this.statisticsProvider == sp) {
            this.statisticsProvider = null;
        }
    }

    private static class NodeStoreWithProps {

        private final NodeStoreProvider nodeStore;

        private final Map<String, ?> props;

        public NodeStoreWithProps(NodeStoreProvider nodeStore, Map<String, ?> props) {
            this.nodeStore = nodeStore;
            this.props = props;
        }

        public NodeStoreProvider getNodeStoreProvider() {
            return nodeStore;
        }

        public Map<String, ?> getProps() {
            return props;
        }

        public String getRole() {
            return PropertiesUtil.toString(props.get(NodeStoreProvider.ROLE), null);
        }

        public String getDescription() {
            return PropertiesUtil.toString(getProps().get("oak.nodestore.description"),
                    getNodeStoreProvider().getClass().toString());
        }
    }
}
