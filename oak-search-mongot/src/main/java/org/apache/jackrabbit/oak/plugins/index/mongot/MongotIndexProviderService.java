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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongotIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(
        service = {Observer.class, QueryIndexProvider.class, IndexEditorProvider.class},
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        property = MongotIndexProviderService.SERVICE_PROPERTY)
@Designate(ocd = MongotIndexProviderService.Config.class)
public final class MongotIndexProviderService
        implements Observer, QueryIndexProvider, IndexEditorProvider {

    static final String SERVICE_TYPE = MongotIndexDefinition.TYPE_MONGOT;
    static final String SERVICE_PROPERTY = "type=" + SERVICE_TYPE;

    @ObjectClassDefinition(
            name = "Oak Mongot Search",
            description = "Native Oak full-text and property index backed by Mongot")
    public @interface Config {

        @AttributeDefinition(name = "MongoDB connection string")
        String connectionString() default "mongodb://localhost:27017/?directConnection=true";

        @AttributeDefinition(name = "MongoDB database")
        String databaseName() default "oak_search";
    }

    private MongoConnection connection;
    private MongotIndexTracker tracker;
    private MongotIndexProvider queryProvider;
    private MongotIndexEditorProvider editorProvider;

    @Activate
    void activate(Config config) {
        connection = MongoConnection.create(config.connectionString(), config.databaseName());
        tracker = new MongotIndexTracker(connection);
        queryProvider = new MongotIndexProvider(tracker);
        editorProvider = new MongotIndexEditorProvider(connection, null);
    }

    @Deactivate
    void deactivate() {
        MongoConnection activeConnection = connection;
        editorProvider = null;
        queryProvider = null;
        tracker = null;
        connection = null;
        if (activeConnection != null) {
            activeConnection.close();
        }
    }

    @Override
    public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
        tracker().contentChanged(root, info);
    }

    @Override
    public @NotNull List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        MongotIndexProvider provider = queryProvider;
        if (provider == null) {
            throw inactive();
        }
        return provider.getQueryIndexes(nodeState);
    }

    @Override
    public @Nullable Editor getIndexEditor(@NotNull String type,
                                           @NotNull NodeBuilder definition,
                                           @NotNull NodeState root,
                                           @NotNull IndexUpdateCallback callback) {
        MongotIndexEditorProvider provider = editorProvider;
        if (provider == null) {
            throw inactive();
        }
        return provider.getIndexEditor(type, definition, root, callback);
    }

    private MongotIndexTracker tracker() {
        MongotIndexTracker activeTracker = tracker;
        if (activeTracker == null) {
            throw inactive();
        }
        return activeTracker;
    }

    private static IllegalStateException inactive() {
        return new IllegalStateException("Mongot service is not active");
    }
}
