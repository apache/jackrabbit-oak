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
package org.apache.jackrabbit.oak.plugins.cow;

import org.apache.jackrabbit.oak.api.jmx.CopyOnWriteStoreMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = {})
public class COWNodeStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(COWNodeStoreService.class);

    /**
     * NodeStoreProvider role
     * <br>
     * Property indicating that this component will not register as a
     * NodeStore but as a NodeStoreProvider with given role
     */
    public static final String PROP_ROLE = "role";

    private NodeStoreProvider nodeStoreProvider;

    private String nodeStoreDescription;

    private ComponentContext context;

    private ServiceRegistration nsReg;

    private Registration mbeanReg;

    private ObserverTracker observerTracker;

    private Whiteboard whiteboard;

    private WhiteboardExecutor executor;

    private String role;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) {
        this.role = PropertiesUtil.toString(config.get(PROP_ROLE), null);
        this.context = context;
        registerNodeStore();
    }

    @Deactivate
    protected void deactivate() {
        unregisterNodeStore();
    }

    private void registerNodeStore() {
        if (nsReg != null) {
            return;
        }
        if (nodeStoreProvider == null) {
            LOG.info("Waiting for the NodeStoreProvider with role=copy-on-write");
            return;
        }
        if (context == null) {
            LOG.info("Waiting for the component activation");
            return;
        }
        COWNodeStore store = new COWNodeStore(nodeStoreProvider.getNodeStore());

        whiteboard = new OsgiWhiteboard(context.getBundleContext());
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        mbeanReg = registerMBean(whiteboard,
                CopyOnWriteStoreMBean.class,
                store.new MBeanImpl(),
                CopyOnWriteStoreMBean.TYPE,
                "Copy-on-write: " + nodeStoreDescription);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, COWNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[]{"nodeStoreType=cowStore"});

        if (role == null) {
            LOG.info("Registering the COW node store");

            observerTracker = new ObserverTracker(store);
            observerTracker.start(context.getBundleContext());

            nsReg = context.getBundleContext().registerService(
                    new String[]{NodeStore.class.getName()},
                    store,
                    props
            );
        } else {
            LOG.info("Registering the COW node store provider");

            props.put("role", role);

            nsReg = context.getBundleContext().registerService(
                    new String[]{NodeStoreProvider.class.getName()},
                    (NodeStoreProvider) () -> store,
                    props
            );
        }
    }

    private void unregisterNodeStore() {
        if (mbeanReg != null) {
            mbeanReg.unregister();
            mbeanReg = null;
        }

        if (executor != null) {
            executor.stop();
            executor = null;
        }

        if (observerTracker != null) {
            observerTracker.stop();
            observerTracker = null;
        }

        if (nsReg != null) {
            LOG.info("Unregistering the COW node store");
            nsReg.unregister();
            nsReg = null;
        }
    }

    @Reference(
            name = "nodeStoreProvider",
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            target = "(role=copy-on-write)")
    protected void bindNodeStoreProvider(NodeStoreProvider ns, Map<String, ?> config) {
        this.nodeStoreProvider = ns;
        this.nodeStoreDescription = PropertiesUtil.toString(config.get("oak.nodestore.description"), ns.getClass().getName());
        registerNodeStore();
    }

    protected void unbindNodeStoreProvider(NodeStoreProvider ns) {
        this.nodeStoreProvider = null;
        this.nodeStoreDescription = null;
        unregisterNodeStore();
    }
}