/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.diffindex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class UUIDDiffIndexProviderWrapper implements QueryIndexProvider {

    private final NodeState before;
    private final NodeState after;

    private final QueryIndexProvider baseProvider;

    public UUIDDiffIndexProviderWrapper(QueryIndexProvider baseProvider,
            NodeState before, NodeState after) {
        this.baseProvider = baseProvider;
        this.before = before;
        this.after = after;
    }

    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<QueryIndex> indexes = new ArrayList<QueryIndex>(
                Collections.singletonList(new UUIDDiffIndex(before, after)));
        indexes.addAll(this.baseProvider.getQueryIndexes(nodeState));
        return indexes;
    }

}
