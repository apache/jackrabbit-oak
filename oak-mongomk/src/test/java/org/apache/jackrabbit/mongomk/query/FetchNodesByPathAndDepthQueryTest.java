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
package org.apache.jackrabbit.mongomk.query;

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.query.FetchNodesByPathAndDepthQuery;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("all")
public class FetchNodesByPathAndDepthQueryTest extends BaseMongoTest {

    @Test
    @Ignore
    public void testFetchWithInvalidFirstRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();
        SimpleNodeScenario scenario2 = new SimpleNodeScenario(mongoConnection);
        String thirdRevisionId = scenario.update_A_and_add_D_and_E();

        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject q = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(firstRevisionId)).get();
        DBObject u = new BasicDBObject("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(q, u);

        q = QueryBuilder.start(CommitMongo.KEY_REVISION_ID).is(MongoUtil.toMongoRepresentation(secondRevisionId))
                .get();
        u = new BasicDBObject("$set", new BasicDBObject(CommitMongo.KEY_BASE_REVISION_ID, 0L));
        commitCollection.update(q, u);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", thirdRevisionId,
                -1);
        List<NodeMongo> result = query.execute();
        List<Node> actuals = NodeMongo.toNode(result);
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%0$s\" : { \"a#%3$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%3$s\" : { \"string\" : \"foo\" , \"e#%3$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"d#%3$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId,
                                thirdRevisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void testFetchWithInvalidLastRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();
        SimpleNodeScenario scenario2 = new SimpleNodeScenario(mongoConnection);
        String thirdRevisionId = scenario.update_A_and_add_D_and_E();

        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject q = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(thirdRevisionId)).get();
        DBObject u = new BasicDBObject();
        u.put("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(q, u);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", thirdRevisionId,
                -1);
        List<NodeMongo> result = query.execute();
        List<Node> actuals = NodeMongo.toNode(result);
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void testFetchWithInvalidMiddleRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();
        SimpleNodeScenario scenario2 = new SimpleNodeScenario(mongoConnection);
        String thirdRevisionId = scenario.update_A_and_add_D_and_E();

        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject q = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(secondRevisionId)).get();
        DBObject u = new BasicDBObject("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(q, u);

        q = QueryBuilder.start(CommitMongo.KEY_REVISION_ID).is(MongoUtil.toMongoRepresentation(thirdRevisionId))
                .get();
        u = new BasicDBObject();
        u.put("$set",
                new BasicDBObject(CommitMongo.KEY_BASE_REVISION_ID, MongoUtil
                        .toMongoRepresentation(firstRevisionId)));
        commitCollection.update(q, u);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", thirdRevisionId,
                -1);
        List<NodeMongo> result = query.execute();
        List<Node> actuals = NodeMongo.toNode(result);
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%3$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%3$s\" : { \"string\" : \"foo\" , \"e#%3$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%3$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId,
                                thirdRevisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void testSimpleFetchRootAndAllDepths() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", firstRevisionId,
                0);
        List<NodeMongo> result = query.execute();
        List<Node> actuals = NodeMongo.toNode(result);
        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", firstRevisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", secondRevisionId, 0);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", firstRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", firstRevisionId, 1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }",
                firstRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", secondRevisionId, 1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 } } }", firstRevisionId,
                secondRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", firstRevisionId, 2);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1, \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", secondRevisionId, 2);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", firstRevisionId, -1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesByPathAndDepthQuery(mongoConnection, "/", secondRevisionId, -1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\", \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }
}
