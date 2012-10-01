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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class FetchNodesByPathAndDepthQueryTest extends BaseMongoTest {

    @Test
    public void fetchWithInvalidFirstRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId1);
        updateBaseRevisionId(revisionId2, 0L);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection,
                "/", revisionId3, -1);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        //String json = String.format("{\"/#%1$s\" : { \"a#%2$s\" : {}, \"b#%3$s\" : {}, \"c#%1$s\" : {} }}", revisionId3, revisionId1, revisionId2);
        String json = String.format("{\"/#%2$s\" : { \"b#%1$s\" : {}, \"c#%2$s\" : {} }}", revisionId2, revisionId3);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithInvalidLastRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId3);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection,
                "/", revisionId3, -1);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        //String json = String.format("{\"/#%1$s\" : { \"a#%2$s\" : {}, \"b#%3$s\" : {}, \"c#%1$s\" : {} }}", revisionId3, revisionId1, revisionId2);
        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"b#%2$s\" : {} }}", revisionId1, revisionId2);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithInvalidMiddleRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId2);
        updateBaseRevisionId(revisionId3, revisionId1);

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection,
                "/", revisionId3, -1);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"c#%2$s\" : {} }}", revisionId1, revisionId3);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void simpleFetchRootAndAllDepths() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        FetchNodesByPathAndDepthQuery query = new FetchNodesByPathAndDepthQuery(mongoConnection,
                "/", firstRevisionId, 0);
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

    private Long addNode(String nodeName) throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", nodeName));
        Commit commit = new CommitImpl("/", "+" + nodeName, "Add /" + nodeName, instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        return command.execute();
    }

    private void invalidateCommit(Long revisionId) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(revisionId).get();
        DBObject update = new BasicDBObject();
        update.put("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(query, update);
    }

    private void updateBaseRevisionId(Long revisionId2, Long baseRevisionId) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(revisionId2)
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject(CommitMongo.KEY_BASE_REVISION_ID, baseRevisionId));
        commitCollection.update(query, update);
    }
}
