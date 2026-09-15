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

import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongoConnectionTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void selectsConfiguredDatabaseAndIndexCollection() {
        MongotIndexDefinition definition = MongotIndexDefinitionTest.newDefinition("siteSearch");

        try (MongoConnection connection = MongoConnection.create(
                mongo.getConnectionString(), mongo.getDatabaseName())) {
            connection.getCollection(definition).insertOne(new Document("_id", "proof"));

            assertEquals(mongo.getDatabaseName(), connection.getDatabase().getName());
            assertEquals(1, connection.getCollection(definition).countDocuments());
            assertEquals(definition.getCollectionName(), connection.getCollection(definition).getNamespace().getCollectionName());
        }
    }
}
