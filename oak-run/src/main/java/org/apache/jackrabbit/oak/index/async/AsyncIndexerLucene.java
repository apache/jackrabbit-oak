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
package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;

import java.util.Arrays;
import java.util.List;


public class AsyncIndexerLucene extends AsyncIndexerBase {



    public AsyncIndexerLucene(NodeStoreFixture fixture, ExtendedIndexHelper extendedIndexHelper, Closer close, List<String> names, long delay) {
        super(fixture, extendedIndexHelper, close, names, delay);
    }

    @Override
    public IndexEditorProvider getIndexEditorProvider() {
        return CompositeIndexEditorProvider
                .compose(Arrays.asList(new LuceneIndexEditorProvider(), new NodeCounterEditorProvider()));
    }
}
