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
package org.apache.jackrabbit.mongomk.impl.action;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesAction;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommand;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.CommitMongo;
import org.apache.jackrabbit.mongomk.impl.model.NodeBuilder;
import org.apache.jackrabbit.mongomk.impl.model.NodeMongo;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class FetchNodesActionTest extends BaseMongoTest {

    @Test
    public void invalidFirstRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId1);
        updateBaseRevisionId(revisionId2, 0L);

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                "/", true, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"b#%1$s\" : {}, \"c#%2$s\" : {} }}",
                revisionId2, revisionId3);
        Iterator<Node> expecteds = NodeBuilder.build(json).getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void invalidLastRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId3);

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                "/", true, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"b#%2$s\" : {} }}",
                revisionId1, revisionId2);
        Iterator<Node> expecteds = NodeBuilder.build(json).getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void invalidMiddleRevision() throws Exception {
        Long revisionId1 = addNode("a");
        Long revisionId2 = addNode("b");
        Long revisionId3 = addNode("c");

        invalidateCommit(revisionId2);
        updateBaseRevisionId(revisionId3, revisionId1);

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                "/", true, revisionId3);
        List<Node> actuals = NodeMongo.toNode(query.execute());

        String json = String.format("{\"/#%2$s\" : { \"a#%1$s\" : {}, \"c#%2$s\" : {} }}",
                revisionId1, revisionId3);
        Iterator<Node> expecteds = NodeBuilder.build(json).getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    // FIXME - Revisit this test.
    @Test
    public void fetchRootAndAllDepths() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                "/", true, firstRevisionId);
        query.setDepth(0);
        List<NodeMongo> result = query.execute();
        List<Node> actuals = NodeMongo.toNode(result);
        String json = String.format("{ \"/#%1$s\" : {} }", firstRevisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, secondRevisionId);
        query.setDepth(0);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : {} }", firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, firstRevisionId);
        query.setDepth(1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }", firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, secondRevisionId);
        query.setDepth(1);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, firstRevisionId);
        query.setDepth(2);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1, \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, secondRevisionId);
        query.setDepth(2);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, firstRevisionId);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection, "/", true, secondRevisionId);
        result = query.execute();
        actuals = NodeMongo.toNode(result);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\", \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    private Long addNode(String nodeName) throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"" + nodeName + "\" : {}", "Add /" + nodeName);
        CommitCommand command = new CommitCommand(mongoConnection, commit);
        return command.execute();
    }

    @Test
    public void fetchWithCertainPathsOneRevision() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long revisionId = scenario.create();

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                getPathSet("/a", "/a/b", "/a/c", "not_existing"), revisionId);
        List<NodeMongo> nodeMongos = query.execute();
        List<Node> actuals = NodeMongo.toNode(nodeMongos);
        String json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                revisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection,
                getPathSet("/a", "not_existing"), revisionId);
        nodeMongos = query.execute();
        actuals = NodeMongo.toNode(nodeMongos);
        json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 } } }",
                revisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    @Test
    public void fetchWithCertainPathsTwoRevisions() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        FetchNodesAction query = new FetchNodesAction(mongoConnection,
                getPathSet("/a", "/a/b", "/a/c", "/a/d", "/a/b/e", "not_existing"),
                firstRevisionId);
        List<NodeMongo> nodeMongos = query.execute();
        List<Node> actuals = NodeMongo.toNode(nodeMongos);
        String json = String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId);
        Node expected = NodeBuilder.build(json);
        Iterator<Node> expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);

        query = new FetchNodesAction(mongoConnection,
                getPathSet("/a", "/a/b", "/a/c", "/a/d", "/a/b/e", "not_existing"),
                secondRevisionId);
        nodeMongos = query.execute();
        actuals = NodeMongo.toNode(nodeMongos);
        json = String.format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId);
        expected = NodeBuilder.build(json);
        expecteds = expected.getChildNodeEntries(0, -1);
        NodeAssert.assertEquals(expecteds, actuals);
    }

    private Set<String> getPathSet(String... paths) {
        return new HashSet<String>(Arrays.asList(paths));
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
