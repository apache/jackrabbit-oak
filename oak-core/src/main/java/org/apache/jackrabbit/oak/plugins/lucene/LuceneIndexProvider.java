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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.QueryIndex;
import org.apache.jackrabbit.oak.spi.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyEditor;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class LuceneIndexProvider implements QueryIndexProvider {

    @Override
    public List<QueryIndex> getQueryIndexes(MicroKernel mk) {
        NodeStore store = new KernelNodeStore(mk, EmptyEditor.INSTANCE);
        return Collections.<QueryIndex>singletonList(new LuceneIndex(store));
    }

}
