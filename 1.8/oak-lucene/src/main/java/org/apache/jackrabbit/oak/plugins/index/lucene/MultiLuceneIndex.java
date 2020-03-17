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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Set;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A full-text search that is used when the query contains there are multiple
 * "parents", as in "contains(a/x, 'hello') and contains(b/x, 'world')". In this
 * case multiple Lucene queries are run, and the result is combined.
 */
public class MultiLuceneIndex {
    
    private final Filter filter;
    private final NodeState root;
    private final Set<String> relPaths;
    
    MultiLuceneIndex(Filter filter, NodeState root,
            Set<String> relPaths) {
        this.filter = filter;
        this.root = root;
        this.relPaths = relPaths;
    }
    
    public String getPlan() {
        return "Not yet implemented";
    }

    public double getCost() {
        return Double.POSITIVE_INFINITY;
    }

    public Cursor query() {
        throw new IllegalStateException(
                "Queries that use multiple relative properties are not yet supported");
    }

}
