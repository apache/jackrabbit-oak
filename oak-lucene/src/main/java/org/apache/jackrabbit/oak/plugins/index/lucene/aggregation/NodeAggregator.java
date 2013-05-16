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
package org.apache.jackrabbit.oak.plugins.index.lucene.aggregation;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getString;

import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;

public class NodeAggregator {

    private final NodeBuilder index;

    public NodeAggregator(NodeBuilder index) {
        this.index = index;
    }

    public List<AggregatedState> getAggregates(NodeState state) {
        // FIXME remove hardcoded aggregates
        String type = getString(state, JCR_PRIMARYTYPE);

        if (NT_FILE.equals(type)) {
            // include jcr:content node
            return ImmutableList.of(new AggregatedState(JCR_CONTENT, state
                    .getChildNode(JCR_CONTENT), null));
        }

        return ImmutableList.of();
    }
}
