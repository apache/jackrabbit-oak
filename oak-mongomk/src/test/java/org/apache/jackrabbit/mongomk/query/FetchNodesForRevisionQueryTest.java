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
public class FetchNodesForRevisionQueryTest extends BaseMongoTest {

    @Test
    public void fetchWithInvalidFirstRevision() throws Exception {
        String revisionId1 = addNode("a");
        String revisionId2 = addNode("b");
        String revisionId3 = addNode("c");

        invalidateCommit(revisionId1);
        updateBaseRevisionId(revisionId2, "0");

        FetchNodesForRevisionQuery query = new FetchNodesForRevisionQuery(mongoConnection,
                new String[] { "/", "/a", "/b", "/c", "not_existing" }, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        //String json = String.format("{\"/#%1$s\" : { \"a#%2$s\" : {}, \"b#%3$s\" : {}, \"c#%1$s\" : {} }}", revisionId3, revisionId1, revisionId2);
        String json = String.format("{\"/#%2$s\" : { \"b#%1$s\" : {}, \"c#%2$s\" : {} }}", revisionId2, revisionId3);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithInvalidLastRevision() throws Exception {
        String revisionId1 = addNode("a");
        String revisionId2 = addNode("b");
        String revisionId3 = addNode("c");

        invalidateCommit(revisionId3);

        FetchNodesForRevisionQuery query = new FetchNodesForRevisionQuery(mongoConnection,
                new String[] { "/", "/a", "/b", "/c", "not_existing" }, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"b#%2$s\" : {} }}", revisionId1, revisionId2);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithInvalidMiddleRevision() throws Exception {
        String revisionId1 = addNode("a");
        String revisionId2 = addNode("b");
        String revisionId3 = addNode("c");

        invalidateCommit(revisionId2);
        updateBaseRevisionId(revisionId3, revisionId1);

        FetchNodesForRevisionQuery query = new FetchNodesForRevisionQuery(mongoConnection,
                new String[] { "/", "/a", "/b", "/c", "not_existing" }, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"c#%2$s\" : {} }}", revisionId1, revisionId3);
        Set<Node> expecteds = NodeBuilder.build(json).getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithOneRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String revisionId = scenario.create();

        FetchNodesForRevisionQuery query = new FetchNodesForRevisionQuery(mongoConnection, new String[] { "/", "/a",
                "/a/b", "/a/c", "not_existing" }, revisionId);
        List<NodeMongo> nodeMongos = query.execute();
        List<Node> actuals = NodeMongo.toNode(nodeMongos);
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                revisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesForRevisionQuery(mongoConnection, new String[] { "/", "/a", "not_existing" }, revisionId);
        nodeMongos = query.execute();
        actuals = NodeMongo.toNode(nodeMongos);
        expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }",
                revisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithTwoRevisions() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();

        FetchNodesForRevisionQuery query = new FetchNodesForRevisionQuery(mongoConnection, new String[] { "/", "/a",
                "/a/b", "/a/c", "/a/d", "/a/b/e", "not_existing" }, firstRevisionId);
        List<NodeMongo> nodeMongos = query.execute();
        List<Node> actuals = NodeMongo.toNode(nodeMongos);
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        Set<Node> expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesForRevisionQuery(mongoConnection, new String[] { "/", "/a", "/a/b", "/a/c", "/a/d",
                "/a/b/e", "not_existing" }, secondRevisionId);
        nodeMongos = query.execute();
        actuals = NodeMongo.toNode(nodeMongos);
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        expecteds = expected.getDescendants(true);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    private String addNode(String nodeName) throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", nodeName));
        Commit commit = new CommitImpl("/", "+" + nodeName, "Add /" + nodeName, instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();
        return revisionId;
    }

    private void invalidateCommit(String revisionId) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(revisionId)).get();
        DBObject update = new BasicDBObject();
        update.put("$set", new BasicDBObject(CommitMongo.KEY_FAILED, Boolean.TRUE));
        commitCollection.update(query, update);
    }

    private void updateBaseRevisionId(String revisionId2, String baseRevisionId) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(revisionId2))
                .get();
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject(CommitMongo.KEY_BASE_REVISION_ID,
                        MongoUtil.toMongoRepresentation(baseRevisionId)));
        commitCollection.update(query, update);
    }
}
