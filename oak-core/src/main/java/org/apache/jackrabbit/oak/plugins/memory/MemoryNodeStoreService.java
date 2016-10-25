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

package org.apache.jackrabbit.oak.plugins.memory;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.jackrabbit.oak.osgi.ObserverTracker;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;

@Component(
        policy = ConfigurationPolicy.REQUIRE,
        label = "Apache Jackrabbit Oak Memory NodeStore Service",
        description = "NodeStore implementation based on MemoryNodeStore. Any changes made with this store would not " +
                "be persisted and would only be visible while system is running. This implementation can be used for " +
                "testing purpose or in those setup where only transient storage is required."
)
public class MemoryNodeStoreService {
    private ObserverTracker observerTracker;
    private ServiceRegistration nodeStoreReg;

    @Activate
    private void activate(BundleContext context) {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        observerTracker = new ObserverTracker(nodeStore);
        observerTracker.start(context);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_DESCRIPTION, "NodeStore implementation based on MemoryNodeStore");
        nodeStoreReg = context.registerService(NodeStore.class.getName(), nodeStore, props);
    }

    @Deactivate
    private void deactivate() {
        if (observerTracker != null) {
            observerTracker.stop();
        }

        if (nodeStoreReg != null) {
            nodeStoreReg.unregister();
        }
    }
}
