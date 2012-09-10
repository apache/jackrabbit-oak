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
package org.apache.jackrabbit.oak.spi.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.jackrabbit.oak.spi.QueryIndex;
import org.apache.jackrabbit.oak.spi.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This {@code QueryIndexProvider} aggregates a list of query index providers
 * into a single query index provider.
 */
public class CompositeQueryIndexProvider implements QueryIndexProvider {

    private final Collection<QueryIndexProvider> providers = new CopyOnWriteArrayList<QueryIndexProvider>();

    public CompositeQueryIndexProvider(QueryIndexProvider... providers) {
        add(providers);
    }

    public void add(QueryIndexProvider... provider) {
        if (provider == null) {
            return;
        }
        for (QueryIndexProvider qip : provider) {
            providers.add(qip);
        }
    }

    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeStore nodeStore) {
        List<QueryIndex> indexes = new ArrayList<QueryIndex>();
        for (QueryIndexProvider qip : providers) {
            List<? extends QueryIndex> t = qip.getQueryIndexes(nodeStore);
            if (t != null) {
                indexes.addAll(t);
            }
        }
        return indexes;
    }

}
