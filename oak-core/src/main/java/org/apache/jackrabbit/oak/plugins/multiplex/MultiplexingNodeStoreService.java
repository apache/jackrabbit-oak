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
package org.apache.jackrabbit.oak.plugins.multiplex;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
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

@Component(policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Oak Multiplexing NodeStore Service",
        description = "NodeStore implementation proxying all the operations " +
                "to other nodestores configured in OSGi"
)
public class MultiplexingNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(MultiplexingNodeStoreService.class);

    private static final String GLOBAL_ROLE = "multiplexing:global";

    private static final String MOUNT_ROLE_PREFIX = "multiplexing:mount:";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY, policy = ReferencePolicy.STATIC)
    private MountInfoProvider mountInfoProvider;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_MULTIPLE, policy = ReferencePolicy.DYNAMIC, bind = "bindNodeStore", unbind = "unbindNodeStore", referenceInterface = NodeStoreProvider.class)
    private List<NodeStoreWithProps> nodeStores = new ArrayList<>();

    private ComponentContext context;

    private ServiceRegistration nsReg;

    @Activate
    protected void activate(ComponentContext context) {
        this.context = context;
        registerMultiplexingNodeStore();
    }

    @Deactivate
    protected void deactivate() {
        unregisterMultiplexingNodeStore();
    }

    private void registerMultiplexingNodeStore() {
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
            LOG.info("Multiplexing node store registration is deferred until there's a global node store registered in OSGi");
            return;
        } else {
            LOG.info("Found global node store: {}", getDescription(globalNs));
        }

        for (Mount m : mountInfoProvider.getNonDefaultMounts()) {
            if (!availableMounts.contains(m.getName())) {
                LOG.info("Multiplexing node store registration is deferred until there's mount {} registered in OSGi", m.getName());
                return;
            }
        }
        LOG.info("Node stores for all configured mounts are available");

        MultiplexingNodeStore.Builder builder = new MultiplexingNodeStore.Builder(mountInfoProvider, globalNs.getNodeStoreProvider().getNodeStore());

        for (NodeStoreWithProps ns : nodeStores) {
            if (isGlobalNodeStore(ns)) {
                continue;
            }
            String mountName = getMountName(ns);
            if (mountName != null) {
                builder.addMount(mountName, ns.getNodeStoreProvider().getNodeStore());
                LOG.info("Mounting {} as {}", getDescription(ns), mountName);
            }
        }

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, MultiplexingNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[] { "nodeStoreType=multiplexing" } );

        LOG.info("Registering the multiplexing node store");

        nsReg = context.getBundleContext().registerService(
                new String[]{
                        NodeStore.class.getName()
                },
                builder.build(),
                props);
    }

    private boolean isGlobalNodeStore(NodeStoreWithProps ns) {
        return GLOBAL_ROLE.equals(ns.getRole());
    }

    private String getMountName(NodeStoreWithProps ns) {
        String role = ns.getRole();
        if (!role.startsWith(MOUNT_ROLE_PREFIX)) {
            return null;
        }
        return role.substring(MOUNT_ROLE_PREFIX.length());
    }

    private String getDescription(NodeStoreWithProps ns) {
        return PropertiesUtil.toString(ns.getProps().get("oak.nodestore.description"), ns.getNodeStoreProvider().getClass().toString());
    }

    private void unregisterMultiplexingNodeStore() {
        if (nsReg != null) {
            LOG.info("Unregistering the multiplexing node store");
            nsReg.unregister();
            nsReg = null;
        }
    }

    protected void bindNodeStore(NodeStoreProvider ns, Map<String, ?> config) {
        NodeStoreWithProps newNs = new NodeStoreWithProps(ns, config);
        nodeStores.add(newNs);

        unregisterMultiplexingNodeStore();
        registerMultiplexingNodeStore();
    }

    protected void unbindNodeStore(NodeStoreProvider ns) {
        Iterator<NodeStoreWithProps> it = nodeStores.iterator();
        while (it.hasNext()) {
            if (it.next().getNodeStoreProvider() == ns) {
                it.remove();
            }
        }

        unregisterMultiplexingNodeStore();
        registerMultiplexingNodeStore();
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