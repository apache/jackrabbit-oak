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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newIdentityHashSet;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(policy = ConfigurationPolicy.REQUIRE)
public class CompositeNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeStoreService.class);

    private static final String GLOBAL_ROLE = "composite:global";

    private static final String MOUNT_ROLE_PREFIX = "composite:mount:";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private MountInfoProvider mountInfoProvider;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_MULTIPLE, policy = ReferencePolicy.DYNAMIC, bind = "bindNodeStore", unbind = "unbindNodeStore", referenceInterface = NodeStoreProvider.class, target="(!(service.pid=org.apache.jackrabbit.oak.composite.CompositeNodeStore))")
    private List<NodeStoreWithProps> nodeStores = new ArrayList<>();

    @Property(label = "Ignore read only writes",
            unbounded = PropertyUnbounded.ARRAY,
            description = "Writes to these read-only paths won't fail the commit"
    )
    private static final String PROP_IGNORE_READ_ONLY_WRITES = "ignoreReadOnlyWrites";

    @Property(label = "Read-only mounts",
            description = "The partial stores should be configured as read-only",
            boolValue = true
    )
    private static final String PROP_PARTIAL_READ_ONLY = "partialReadOnly";

    private ComponentContext context;

    private Set<NodeStoreProvider> nodeStoresInUse = newIdentityHashSet();

    private ServiceRegistration nsReg;

    private Registration checkpointReg;

    private ObserverTracker observerTracker;

    private String[] ignoreReadOnlyWritePaths;

    private boolean partialReadOnly;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) {
        this.context = context;
        ignoreReadOnlyWritePaths = PropertiesUtil.toStringArray(config.get(PROP_IGNORE_READ_ONLY_WRITES), new String[0]);
        partialReadOnly = PropertiesUtil.toBoolean(config.get(PROP_PARTIAL_READ_ONLY), true);
        registerCompositeNodeStore();
    }

    @Deactivate
    protected void deactivate() {
        unregisterCompositeNodeStore();
    }

    private void registerCompositeNodeStore() {
        if (nsReg != null) {
            return; // already registered
        }

        NodeStoreWithProps globalNs = null;
        Set<String> availableMounts = new HashSet<>();
        for (NodeStoreWithProps ns : nodeStores) {
            if (isGlobalNodeStore(ns)) {
                globalNs = ns;
            } else {
                availableMounts.add(getMountName(ns));
            }
        }

        if (globalNs == null) {
            LOG.info("Composite node store registration is deferred until there's a global node store registered in OSGi");
            return;
        } else {
            LOG.info("Found global node store: {}", getDescription(globalNs));
        }

        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            if (!availableMounts.contains(m.getName())) {
                LOG.info("Composite node store registration is deferred until there's mount {} registered in OSGi", m.getName());
                return;
            }
        }
        LOG.info("Node stores for all configured mounts are available");

        CompositeNodeStore.Builder builder = new CompositeNodeStore.Builder(mountInfoProvider, globalNs.getNodeStoreProvider().getNodeStore());
        nodeStoresInUse.add(globalNs.getNodeStoreProvider());

        builder.setPartialReadOnly(partialReadOnly);
        for (String p : ignoreReadOnlyWritePaths) {
            builder.addIgnoredReadOnlyWritePath(p);
        }

        for (NodeStoreWithProps ns : nodeStores) {
            if (isGlobalNodeStore(ns)) {
                continue;
            }
            String mountName = getMountName(ns);
            if (mountName != null) {
                builder.addMount(mountName, ns.getNodeStoreProvider().getNodeStore());
                LOG.info("Mounting {} as {}", getDescription(ns), mountName);
                nodeStoresInUse.add(ns.getNodeStoreProvider());
            }
        }

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, CompositeNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[] { "nodeStoreType=compositeStore" } );

        CompositeNodeStore store = builder.build();

        observerTracker = new ObserverTracker(store);
        observerTracker.start(context.getBundleContext());

        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        checkpointReg = registerMBean(whiteboard,
                CheckpointMBean.class,
                new CompositeCheckpointMBean(store),
                CheckpointMBean.TYPE,
                "Composite node store checkpoint management");

        LOG.info("Registering the composite node store");
        nsReg = context.getBundleContext().registerService(
                new String[]{
                        NodeStore.class.getName()
                },
                store,
                props);
    }

    private boolean isGlobalNodeStore(NodeStoreWithProps ns) {
        return GLOBAL_ROLE.equals(ns.getRole());
    }

    private String getMountName(NodeStoreWithProps ns) {
        String role = ns.getRole();
        if (role == null) {
            return null;
        }
        if (!role.startsWith(MOUNT_ROLE_PREFIX)) {
            return null;
        }
        return role.substring(MOUNT_ROLE_PREFIX.length());
    }

    private String getDescription(NodeStoreWithProps ns) {
        return PropertiesUtil.toString(ns.getProps().get("oak.nodestore.description"), ns.getNodeStoreProvider().getClass().toString());
    }

    private void unregisterCompositeNodeStore() {
        if (nsReg != null) {
            LOG.info("Unregistering the composite node store");
            nsReg.unregister();
            nsReg = null;
        }
        if (checkpointReg != null) {
            checkpointReg.unregister();
            checkpointReg = null;
        }
        if (observerTracker != null) {
            observerTracker.stop();
            observerTracker = null;
        }
        nodeStoresInUse.clear();
    }

    protected void bindNodeStore(NodeStoreProvider ns, Map<String, ?> config) {
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

    protected void unbindNodeStore(NodeStoreProvider ns) {
        Iterator<NodeStoreWithProps> it = nodeStores.iterator();
        while (it.hasNext()) {
            if (it.next().getNodeStoreProvider() == ns) {
                it.remove();
            }
        }

        if (context == null) {
            LOG.info("unbindNodeStore: context is null, delaying reconfiguration");
            return;
        }

        if (nsReg != null && nodeStoresInUse.contains(ns)) {
            unregisterCompositeNodeStore();
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
    }
}