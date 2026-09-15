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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public final class MongotIndexWriterFactory implements FulltextIndexWriterFactory<MongoDocument> {

    private static final String SYNC_MODE_PROPERTY = "sync-mode";
    private static final String SYNC_RT_MODE = "rt";

    private final MongoConnection connection;

    public MongotIndexWriterFactory(MongoConnection connection) {
        this.connection = connection;
    }

    @Override
    public FulltextIndexWriter<MongoDocument> newInstance(IndexDefinition definition,
                                                           NodeBuilder definitionBuilder,
                                                           CommitInfo commitInfo,
                                                           boolean reindex) {
        if (!(definition instanceof MongotIndexDefinition)) {
            throw new IllegalArgumentException("IndexDefinition must be a MongotIndexDefinition");
        }
        return new MongotIndexWriter(connection, (MongotIndexDefinition) definition,
                definitionBuilder, reindex, isRealTime(definition, commitInfo));
    }

    static boolean isRealTime(IndexDefinition definition, CommitInfo commitInfo) {
        if (definition.getDefinitionNodeState().hasProperty(
                IndexConstants.ASYNC_PROPERTY_NAME)) {
            return false;
        }
        Object commitMode = commitInfo == null
                ? null
                : commitInfo.getInfo().get(SYNC_MODE_PROPERTY);
        if (commitMode != null) {
            return SYNC_RT_MODE.equals(commitMode);
        }
        PropertyState configured = definition.getDefinitionNodeState()
                .getProperty(SYNC_MODE_PROPERTY);
        return configured != null
                && SYNC_RT_MODE.equals(configured.getValue(Type.STRING));
    }
}
