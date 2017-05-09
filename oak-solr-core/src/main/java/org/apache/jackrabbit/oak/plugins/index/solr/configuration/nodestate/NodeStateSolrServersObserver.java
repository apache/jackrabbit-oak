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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DiffObserver;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link org.apache.jackrabbit.oak.spi.commit.Observer} looking for changes on persisted Solr server configuration nodes.
 * If any change is done there, the related {@link org.apache.solr.client.solrj.SolrServer}s are shutdown and unregistered
 * from the {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerRegistry}
 */
public class NodeStateSolrServersObserver extends DiffObserver {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected NodeStateDiff getRootDiff(@Nonnull NodeState before, @Nonnull NodeState after, @Nonnull CommitInfo info) {
        return new ChangingSolrServersNodeStateDiff(after);
    }

    private void shutdownRegisteredSolrServers(NodeState nodeState) {
        log.debug("shutting down persisted Solr server");
        NodeStateSolrServerConfigurationProvider nodeStateSolrServerConfigurationProvider = new NodeStateSolrServerConfigurationProvider(nodeState);
        OakSolrServer oakSolrServer = new OakSolrServer(nodeStateSolrServerConfigurationProvider);
        oakSolrServer.shutdown();
        log.info("persisted Solr server has been shutdown");
    }

    private class ChangingSolrServersNodeStateDiff implements NodeStateDiff {
        private final NodeState nodeState;
        private final String name;

        public ChangingSolrServersNodeStateDiff(NodeState after) {
            nodeState = after;
            name = "";
        }

        public ChangingSolrServersNodeStateDiff(NodeState nodeState, String name) {
            this.nodeState = nodeState;
            this.name = name;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (isSolrServerNode(name, nodeState)) {
                shutdownRegisteredSolrServers(nodeState);
            }
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (isSolrServerNode(name, nodeState)) {
                shutdownRegisteredSolrServers(nodeState);
            }
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (isSolrServerNode(name, nodeState)) {
                shutdownRegisteredSolrServers(nodeState);
            }
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (isSolrServerNode(name, before)) {
                shutdownRegisteredSolrServers(before);
            }
            return after.compareAgainstBaseState(before, new ChangingSolrServersNodeStateDiff(after, this.name + "/" + name));
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (isSolrServerNode(name, before)) { // look if the deleted node was a server node
                shutdownRegisteredSolrServers(before);
            } else { //look among child nodes if there was a server node
                for (String childNodeName : before.getChildNodeNames()) {
                    NodeState childNodeState = before.getChildNode(childNodeName);
                    if (isSolrServerNode(childNodeName, childNodeState)) {
                        shutdownRegisteredSolrServers(childNodeState);
                        break;
                    }
                }
            }
            return true;
        }

        private boolean isSolrServerNode(String name, NodeState nodeState) {
            log.info("checking {} in {}", name, nodeState);
            return "server".equals(name) && nodeState.hasProperty("solrServerType");
        }

    }
}
