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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import java.io.IOException;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;

/**
 * A {@link CommitHook} which sends changes to Apache Solr
 */
public class SolrCommitHook implements CommitHook {

    private final SolrServer solrServer;

    public SolrCommitHook(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after) throws CommitFailedException {
        try {
            SolrNodeStateDiff diff = new SolrNodeStateDiff(solrServer);
            after.compareAgainstBaseState(before, diff);
            diff.postProcess(after);
            return after;
        } catch (IOException e) {
            throw new CommitFailedException(
                    "Failed to update the Solr index", e);
        }
    }

}
