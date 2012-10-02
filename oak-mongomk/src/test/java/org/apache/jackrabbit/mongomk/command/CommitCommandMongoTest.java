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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.MongoAssert;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.builder.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class CommitCommandMongoTest extends BaseMongoTest {

    @Test
    public void addNodes() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {} , \"c\" : {} }",
                "This is a simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"b#%1$s\" : {} , \"c#%1$s\" : {} } } }", revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertHeadRevision(1);
        MongoAssert.assertNextRevision(2);
    }

    @Test
    public void addNodesToSameParent() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : {}", "This is the 1st commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long firstRevisionId = command.execute();

        commit = CommitBuilder.build("/", "+\"b\" : {}", "This is the 2nd commit");
        command = new CommitCommandMongo(mongoConnection, commit);
        Long secondRevisionId = command.execute();

        commit = CommitBuilder.build("/", "+\"c\" : {}", "This is the 3rd commit");
        command = new CommitCommandMongo(mongoConnection, commit);
        Long thirdRevisionId = command.execute();

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%3$s\" : { \"a#%1$s\" : { } , \"b#%2$s\" : { } , \"c#%3$s\" : { } } }",
                firstRevisionId, secondRevisionId, thirdRevisionId)));
    }

    @Test
    public void addIntermediataryNodes() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : { \"c\": {} }}",
                "Add /a/b/c");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId1 = command.execute();

        commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : { \"e\": {} }, \"d\" : {} }",
                "Add /a/d and /a/b/e");
        command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId2 = command.execute();

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%2$s\" : { \"b#%2$s\" : { \"c#%1$s\" : {}, \"e#%2$s\" : {} }, " +
                " \"d#%2$s\" : {} } } }", revisionId1, revisionId2)));
    }

    @Test
    public void addDuplicateNode() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {} }", "Add a/b");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        command.execute();

        commit = CommitBuilder.build("/a", "+\"b\" : {}", "Add a/b again");
        command = new CommitCommandMongo(mongoConnection, commit);
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {
        }
    }

    @Test
    public void addNodesAndProperties() throws Exception {
        SimpleNodeScenario scenario1 = new SimpleNodeScenario(mongoConnection);
        scenario1.create();

        Commit commit = CommitBuilder.build("/",
                "+\"a\" : { \"key1\" : \"value1\" , \"b\" : {\"key2\" : \"value2\"}"
                + ", \"c\" : {\"key3\" : \"value3\"}}",
                "This is a simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : {  \"int\" : 1 , \"key1\" : \"value1\", \"b#%1$s\" : { \"string\" : \"foo\" , \"key2\" : \"value2\" } , \"c#%1$s\" : { \"bool\" : true , \"key3\" : \"value3\" } } } }",
                revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/", "/a", "/a/b", "/a/c");
    }

    @Test
    @Ignore
    // FIXME - This currently fails due to some limit in property sizes in Mongo
    // which affects path property.
    public void bigCommit() throws Exception {
        String path = "/";
        String baseNodeName = "test";
        int numberOfCommits = 1000;

        for (int i = 0; i < numberOfCommits; i++) {
            Commit commit = CommitBuilder.build(path, "+\"" + baseNodeName + i + "\" : {}",
                    "Add node n" + i);
            CommitCommandMongo command = new CommitCommandMongo(
                    mongoConnection, commit);
            command.execute();
            if (!PathUtils.denotesRoot(path)) {
                path += "/";
            }
            path += baseNodeName + i;
        }
    }

    @Test
    public void commitAndMergeNodes() throws Exception {
        SimpleNodeScenario scenario1 = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario1.create();
        Long secondRevisionId = scenario1.update_A_and_add_D_and_E();

        SimpleNodeScenario scenario2 = new SimpleNodeScenario(mongoConnection);
        Long thirdRevisionId = scenario2.create();

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                firstRevisionId)));

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId)));

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%3$s\" : { \"a#%3$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%3$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%3$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                firstRevisionId, secondRevisionId, thirdRevisionId)));
    }

    @Test
    public void commitContainsAllAffectedNodes() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();

        MongoAssert.assertCommitContainsAffectedPaths(firstRevisionId, "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertCommitContainsAffectedPaths(secondRevisionId, "/a", "/a/b", "/a/d", "/a/b/e");
    }

    @Test
    public void existingParentContainsChildren() throws Exception {
        Commit commit = CommitBuilder.build("", "+ \"/\" : {\"a\" : {}, \"b\" : {}, \"c\" : {}}",
                "This is a simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();

        assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : {}, \"b#%1$s\" : {} , \"c#%1$s\" : {} } }", revisionId)));

        GetNodesCommandMongo command2 = new GetNodesCommandMongo(mongoConnection, "/", revisionId, 0);
        Node rootOfPath = command2.execute();
        assertEquals(3, rootOfPath.getChildCount());
    }

    @Test
    public void mergePropertiesAndChildrenNoneExistedAndNewAdded() throws Exception {
        Commit commit = CommitBuilder.build("/",
                "+\"a\" : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", "0")));
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"key1\" : \"value1\", \"key2\" : \"value2\", \"key3\" : \"value3\" } } }",
                revisionId)));
    }

    @Test
    public void mergePropertiesAndChildrenSomeExistedAndNewAdded() throws Exception {
        Commit commit = CommitBuilder.build("/",
                "+\"a\" : { \"existed_key1\" : \"value1\" , \"existed_key2\" : \"value2\" , \"existed_key3\" : \"value3\" }",
                "This is a simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();

        commit = CommitBuilder.build("/",
                "+\"a\" : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit");
        command = new CommitCommandMongo(mongoConnection, commit);
        revisionId = command.execute();

        MongoAssert.assertNodesExist(NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", "0")));
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"existed_key1\" : \"value1\", \"existed_key2\" : \"value2\", \"existed_key3\" : \"value3\", \"key1\" : \"value1\", \"key2\" : \"value2\", \"key3\" : \"value3\" } } }",
                revisionId)));
    }

    @Test
    public void noOtherNodesTouched() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : {}" +
                "\n+\"b\" : {}" + "\n+\"c\" : {}", "Simple commit");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long firstRevisionId = command.execute();

        commit = CommitBuilder.build("/a", "+\"d\": {} \n+\"e\" : {}", "This is a simple commit");
        command = new CommitCommandMongo(mongoConnection, commit);
        Long secondRevisionId = command.execute();

        MongoAssert.assertNodeRevisionId("/", firstRevisionId, true);
        MongoAssert.assertNodeRevisionId("/a", firstRevisionId, true);
        MongoAssert.assertNodeRevisionId("/b", firstRevisionId, true);
        MongoAssert.assertNodeRevisionId("/c", firstRevisionId, true);
        MongoAssert.assertNodeRevisionId("/a/d", firstRevisionId, false);
        MongoAssert.assertNodeRevisionId("/a/e", firstRevisionId, false);

        MongoAssert.assertNodeRevisionId("/", secondRevisionId, false);
        MongoAssert.assertNodeRevisionId("/a", secondRevisionId, true);
        MongoAssert.assertNodeRevisionId("/b", secondRevisionId, false);
        MongoAssert.assertNodeRevisionId("/c", secondRevisionId, false);
        MongoAssert.assertNodeRevisionId("/a/d", secondRevisionId, true);
        MongoAssert.assertNodeRevisionId("/a/e", secondRevisionId, true);
    }

    @Test
    public void removeNode() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {} , \"c\" : {} }",
                "Add a and its children");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();
        assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"b#%1$s\" : {} , \"c#%1$s\" : {} } } }", revisionId)));

        commit = CommitBuilder.build("/", "-\"a\"", "Remove a");
        command = new CommitCommandMongo(mongoConnection, commit);
        revisionId = command.execute();
        assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }",
                revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/");
    }

    @Test
    public void removeNonExistentNode() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {}  }", "Add nodes");
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        command.execute();

        commit = CommitBuilder.build("/a", "-\"c\"", "Non-existent node delete");
        command = new CommitCommandMongo(mongoConnection, commit);
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {

        }
    }

    @Test
    public void rootNodeHasEmptyRootPath() throws Exception {
        Commit commit = CommitBuilder.build(MongoUtil.INITIAL_COMMIT_PATH, MongoUtil.INITIAL_COMMIT_DIFF,
                MongoUtil.INITIAL_COMMIT_MESSAGE);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long revisionId = command.execute();
        assertNotNull(revisionId);

        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", revisionId));
        MongoAssert.assertNodesExist(expected);
    }
}