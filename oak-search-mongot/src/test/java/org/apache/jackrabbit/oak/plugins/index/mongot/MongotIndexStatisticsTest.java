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

import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoDocument;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotIndexStatisticsTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void planningCountsQueryMongoDBOncePerCount() {
        MongotIndexDefinition definition = MongotIndexDefinitionTest.newDefinition("statistics");
        MongoDocument published = new MongoDocument("/published");
        published.addTypedProperty("status", "published");
        mongo.getSearchConnection().getCollection(definition).insertMany(List.of(
                published.toBson(), new MongoDocument("/draft").toBson()));
        MongotIndexStatistics statistics =
                new MongotIndexStatistics(mongo.getSearchConnection(), definition);

        long before = countQueries();
        assertEquals(2, statistics.numDocs());
        assertEquals(1, statistics.getDocCountFor("status"));
        assertEquals(before + 2, countQueries());

        // Oak's planner asks again for every query; those reads must not reach MongoDB.
        assertEquals(2, statistics.numDocs());
        assertEquals(1, statistics.getDocCountFor("status"));
        assertEquals(before + 2, countQueries());
    }

    // countDocuments runs as an aggregation with a $group stage.
    private static long countQueries() {
        Number count = mongo.getDatabase().runCommand(new Document("serverStatus", 1))
                .get("metrics", Document.class).get("aggStageCounters", Document.class)
                .get("$group", Number.class);
        return count == null ? 0 : count.longValue();
    }
}
