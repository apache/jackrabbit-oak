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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.util.ArrayList;
import java.util.List;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

/**
 * An OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver}.
 * This allows correct cleanup of any persisted Solr server configurations once they get changed or deleted.
 */
@Component(metatype = true,
        immediate = true,
        label = "Apache Jackrabbit Oak Solr persisted configuration observer"
)
public class NodeStateSolrServersObserverService {

    private final NodeStateSolrServersObserver nodeStateSolrServersObserver = new NodeStateSolrServersObserver();

    private WhiteboardExecutor executor;

    private BackgroundObserver backgroundObserver;

    private List<ServiceRegistration> regs = new ArrayList<ServiceRegistration>();

    @Property(boolValue = false, label = "enabled", description = "enable persisted configuration observer")
    private static final String ENABLED = "enabled";

    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {

        boolean enabled = PropertiesUtil.toBoolean(componentContext.getProperties().get(ENABLED), false);

        if (enabled) {
            BundleContext bundleContext = componentContext.getBundleContext();
            Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
            executor = new WhiteboardExecutor();
            executor.start(whiteboard);

            backgroundObserver = new BackgroundObserver(nodeStateSolrServersObserver, executor, 5);
            regs.add(bundleContext.registerService(Observer.class.getName(), backgroundObserver, null));
        }
    }

    @Deactivate
    protected void deactivate() throws Exception {

        for (ServiceRegistration reg : regs) {
            reg.unregister();
        }

        if (backgroundObserver != null) {
            backgroundObserver.close();
        }

        if (executor != null) {
            executor.stop();
        }

    }
}
