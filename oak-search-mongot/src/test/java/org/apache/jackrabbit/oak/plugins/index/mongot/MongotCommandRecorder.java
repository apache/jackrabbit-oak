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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;

/**
 * Records the aggregations a connector sends through its own client, so tests observe
 * only the connector's commands and need no server-wide privileges.
 */
final class MongotCommandRecorder implements CommandListener {

    private final List<BsonDocument> aggregations = new CopyOnWriteArrayList<>();

    MongoConnection connect(String connectionString, String databaseName) {
        return MongoConnection.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .addCommandListener(this)
                .build(), databaseName);
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        if ("aggregate".equals(event.getCommandName())) {
            aggregations.add(event.getCommand().clone());
        }
    }

    long stageCount(String stage) {
        return aggregations.stream()
                .filter(command -> command.getArray("pipeline").stream()
                        .anyMatch(step -> step.asDocument().containsKey(stage)))
                .count();
    }
}
