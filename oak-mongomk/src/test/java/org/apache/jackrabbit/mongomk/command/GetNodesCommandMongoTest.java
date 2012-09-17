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
package org.apache.jackrabbit.mongomk.command;

import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.NodeAssert;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class GetNodesCommandMongoTest extends BaseMongoTest {

    @Test
    public void testGetNodesAfterDeletion() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String revisionId = scenario.create();
        revisionId = scenario.delete_A();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/", revisionId, -1);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", revisionId));

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    @Ignore
    public void testGetNodeWithMissingLeafNode() throws Exception {
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%1$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                "1", "2"));
        MongoConnection mockConnection = createMissingNodeScenario(expected, "/a/c");

        GetNodesCommandMongo command = new GetNodesCommandMongo(mockConnection, "/", "2", -1);
        Node actual = command.execute();

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    @Ignore
    public void testGetNodeWithMissingParentNode() throws Exception {
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%1$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                "1", "2"));
        MongoConnection mockConnection = createMissingNodeScenario(expected, "/a");

        GetNodesCommandMongo command = new GetNodesCommandMongo(mockConnection, "/", "2", -1);
        Node actual = command.execute();

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    @Ignore
    public void testGetNodeWithStaleLeafNode() throws Exception {
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%1$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                "1", "2"));
        MongoConnection mockConnection = createStaleNodeScenario(expected, "/a/c");

        GetNodesCommandMongo command = new GetNodesCommandMongo(mockConnection, "/", "2", -1);
        Node actual = command.execute();

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    @Ignore
    public void testGetNodeWithStaleParentNode() throws Exception {
        Node expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%1$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                "1", "2"));
        MongoConnection mockConnection = createStaleNodeScenario(expected, "/a");

        GetNodesCommandMongo command = new GetNodesCommandMongo(mockConnection, "/", "2", -1);
        Node actual = command.execute();

        NodeAssert.assertDeepEquals(expected, actual);
    }

    @Test
    public void testSimpleGetNodes() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();

        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, 0);
        Node actual = command.execute();
        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a\" : {} } }",
                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 0);
        actual = command.execute();
        expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a\" : {} } }", firstRevisionId,
                secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, 1);
        actual = command.execute();
        expected = NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b\" : {} , \"c\" : {} } } }",
                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 1);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123, \"b\" : {} , \"c\" : {} , \"d\" : {} } } }",
                                firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, 2);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, 2);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e\" : {} } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", firstRevisionId, -1);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                firstRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);

        command = new GetNodesCommandMongo(mongoConnection, "/", secondRevisionId, -1);
        actual = command.execute();
        expected = NodeBuilder
                .build(String
                        .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                firstRevisionId, secondRevisionId));
        NodeAssert.assertDeepEquals(expected, actual);
    }

    private MongoConnection createMissingNodeScenario(Node expected, String missingPath) {
        BasicDBList results1 = new BasicDBList();
        BasicDBList results2 = new BasicDBList();

        Set<NodeMongo> expectedNodeMongos = NodeMongo.fromNodes(expected.getDescendants(true));
        for (NodeMongo nodeMongo : expectedNodeMongos) {

            BasicDBObject groupDbObject = new BasicDBObject();
            groupDbObject.put("result", nodeMongo);
            if (!nodeMongo.getPath().equals(missingPath)) {
                results1.add(groupDbObject);
            }
            results2.add(groupDbObject);
        }

        DBCollection mockNodeCollection = EasyMock.createMock(DBCollection.class);
        EasyMock.expect(
                mockNodeCollection.group(EasyMock.anyObject(DBObject.class), EasyMock.anyObject(DBObject.class),
                        EasyMock.anyObject(DBObject.class), EasyMock.anyObject(String.class))).andReturn(results1)
                .once();
        EasyMock.expect(
                mockNodeCollection.group(EasyMock.anyObject(DBObject.class), EasyMock.anyObject(DBObject.class),
                        EasyMock.anyObject(DBObject.class), EasyMock.anyObject(String.class))).andReturn(results2)
                .once();
        EasyMock.replay(mockNodeCollection);

        CommitMongo firstCommit = new CommitMongo();
        firstCommit.setAffectedPaths(Arrays.asList(new String[] { "/", "/a", "/a/b", "/a/c" }));
        firstCommit.setRevisionId(1L);

        CommitMongo secondCommit = new CommitMongo();
        secondCommit.setAffectedPaths(Arrays.asList(new String[] { "/a", "/a/d", "/a/b/e" }));
        secondCommit.setRevisionId(2L);

        DBCursor mockDbCursor = EasyMock.createMock(DBCursor.class);
        EasyMock.expect(mockDbCursor.sort(EasyMock.anyObject(DBObject.class))).andReturn(mockDbCursor);
        EasyMock.expect(mockDbCursor.limit(EasyMock.anyInt())).andReturn(mockDbCursor);
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(true).once();
        EasyMock.expect(mockDbCursor.next()).andReturn(firstCommit).once();
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(true).once();
        EasyMock.expect(mockDbCursor.next()).andReturn(secondCommit).once();
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(false).once();
        EasyMock.replay(mockDbCursor);

        DBCollection mockCommitCollection = EasyMock.createMock(DBCollection.class);
        EasyMock.expect(mockCommitCollection.find(EasyMock.anyObject(DBObject.class))).andReturn(mockDbCursor);
        EasyMock.replay(mockCommitCollection);

        MongoConnection mockConnection = EasyMock.createMock(MongoConnection.class);
        EasyMock.expect(mockConnection.getNodeCollection()).andReturn(mockNodeCollection).times(2);
        EasyMock.expect(mockConnection.getCommitCollection()).andReturn(mockCommitCollection);
        EasyMock.replay(mockConnection);

        return mockConnection;
    }

    private MongoConnection createStaleNodeScenario(Node expected, String stalePath) {
        BasicDBList results1 = new BasicDBList();
        BasicDBList results2 = new BasicDBList();

        Set<NodeMongo> expectedNodeMongos = NodeMongo.fromNodes(expected.getDescendants(true));
        for (NodeMongo nodeMongo : expectedNodeMongos) {
            BasicDBObject groupDbObject = new BasicDBObject();
            groupDbObject.put("result", nodeMongo);
            results2.add(groupDbObject);

            if (nodeMongo.getPath().equals(stalePath)) {
                NodeMongo nodeMongoStale = new NodeMongo();
                nodeMongoStale.putAll(nodeMongo.toMap());
                nodeMongoStale.setRevisionId(1L);
            }

            results1.add(groupDbObject);
        }

        DBCollection mockNodeCollection = EasyMock.createMock(DBCollection.class);
        EasyMock.expect(
                mockNodeCollection.group(EasyMock.anyObject(DBObject.class), EasyMock.anyObject(DBObject.class),
                        EasyMock.anyObject(DBObject.class), EasyMock.anyObject(String.class))).andReturn(results1)
                .once();
        EasyMock.expect(
                mockNodeCollection.group(EasyMock.anyObject(DBObject.class), EasyMock.anyObject(DBObject.class),
                        EasyMock.anyObject(DBObject.class), EasyMock.anyObject(String.class))).andReturn(results2)
                .once();
        EasyMock.replay(mockNodeCollection);

        CommitMongo firstCommit = new CommitMongo();
        firstCommit.setAffectedPaths(Arrays.asList(new String[] { "/", "/a", "/a/b", "/a/c" }));
        firstCommit.setRevisionId(1L);

        CommitMongo secondCommit = new CommitMongo();
        secondCommit.setAffectedPaths(Arrays.asList(new String[] { "/a", "/a/d", "/a/b/e" }));
        secondCommit.setRevisionId(2L);

        DBCursor mockDbCursor = EasyMock.createMock(DBCursor.class);
        EasyMock.expect(mockDbCursor.sort(EasyMock.anyObject(DBObject.class))).andReturn(mockDbCursor);
        EasyMock.expect(mockDbCursor.limit(EasyMock.anyInt())).andReturn(mockDbCursor);
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(true).once();
        EasyMock.expect(mockDbCursor.next()).andReturn(firstCommit).once();
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(true).once();
        EasyMock.expect(mockDbCursor.next()).andReturn(secondCommit).once();
        EasyMock.expect(mockDbCursor.hasNext()).andReturn(false).once();
        EasyMock.replay(mockDbCursor);

        DBCollection mockCommitCollection = EasyMock.createMock(DBCollection.class);
        EasyMock.expect(mockCommitCollection.find(EasyMock.anyObject(DBObject.class))).andReturn(mockDbCursor);
        EasyMock.replay(mockCommitCollection);

        MongoConnection mockConnection = EasyMock.createMock(MongoConnection.class);
        EasyMock.expect(mockConnection.getNodeCollection()).andReturn(mockNodeCollection).times(2);
        EasyMock.expect(mockConnection.getCommitCollection()).andReturn(mockCommitCollection);
        EasyMock.replay(mockConnection);

        return mockConnection;
    }
}
