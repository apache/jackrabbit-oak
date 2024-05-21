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
package org.apache.jackrabbit.oak.indexversion;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class LucenePurgeOldIndexVersion extends PurgeOldIndexVersion {

    @Override
    protected String getIndexType() {
        return TYPE_LUCENE;
    }

    @Override
    protected void postDeleteOp(String idxPath) {
        // NOOP
    }

    @Override
    protected void preserveDetailsFromIndexDefForPostOp(NodeBuilder builder) {
        // NOOP
    }

    @Override
    protected IndexVersionOperation getIndexVersionOperationInstance(IndexName indexName) {
        return new LuceneIndexVersionOperation(indexName);
    }
}
