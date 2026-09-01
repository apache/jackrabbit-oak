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
package org.apache.jackrabbit.oak.plugins.index.nodetype;

import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider.FT_OAK_12348;
import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature;

import java.util.List;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

/**
 * <code>NodeTypeIndexProvider</code> is a {@link QueryIndexProvider} for
 * {@link NodeTypeIndex} instances.
 */
@Component(service = QueryIndexProvider.class)
public class NodeTypeIndexProvider implements QueryIndexProvider {

    @Reference
    private MountInfoProvider mountInfoProvider = Mounts
            .defaultMountInfoProvider();

    /**
     * See {@link org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider#FT_OAK_12348}
     * (OAK-12348) -- registered independently here rather than shared with
     * PropertyIndexProvider, since the two are separate, independently
     * activated components with no existing cross-component wiring in this
     * codebase. Same toggle name, so an operator flipping one knows to check
     * the other.
     */
    @Nullable
    private Feature feature;

    @Activate
    private void activate(BundleContext bundleContext) {
        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        this.feature = newFeature(FT_OAK_12348, whiteboard);
    }

    @Deactivate
    private void deactivate() {
        if (feature != null) {
            feature.close();
            feature = null;
        }
    }

    @NotNull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        return List.of(new NodeTypeIndex(mountInfoProvider, feature));
    }

    public NodeTypeIndexProvider with(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
        return this;
    }
}
