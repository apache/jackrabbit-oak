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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServersObserver}
 */
@Component(immediate = true)
@Service(value = Observer.class)
public class NodeStateSolrServersObserverService implements Observer {

    private final NodeStateSolrServersObserver nodeStateSolrServersObserver = new NodeStateSolrServersObserver();

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        nodeStateSolrServersObserver.contentChanged(root, info);
    }
}
