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

import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.MongoAssert;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Instruction;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.impl.model.AddNodeInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.AddPropertyInstructionImpl;
import org.apache.jackrabbit.mongomk.impl.model.CommitImpl;
import org.apache.jackrabbit.mongomk.impl.model.RemoveNodeInstructionImpl;
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
    @Ignore // FIXME - Implement
    public void addIntermediataryNodes() throws Exception {
    }

    @Test
    public void addNewNodesToSameParent() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "1"));

        Commit commit = new CommitImpl("/", "+1 : {}", "This is the 1st commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String firstRevisionId = command.execute();

        instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "2"));

        commit = new CommitImpl("/", "+2 : {}", "This is the 2nd commit", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        String secondRevisionId = command.execute();

        instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "3"));

        commit = new CommitImpl("/", "+3 : {}", "This is the 3rd commit", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        String thirdRevisionId = command.execute();

        MongoAssert.assertNodesExist("", NodeBuilder.build(String.format(
                "{ \"/#%3$s\" : { \"1#%1$s\" : { } , \"2#%2$s\" : { } , \"3#%3$s\" : { } } }",
                firstRevisionId, secondRevisionId, thirdRevisionId)));
    }

    @Test
    public void addNodes() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        instructions.add(new AddNodeInstructionImpl("/a", "c"));

        Commit commit = new CommitImpl("/", "+a : { b : {} , c : {} }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert.assertNodesExist("", NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"b#%1$s\" : {} , \"c#%1$s\" : {} } } }", revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertHeadRevision(1);
        MongoAssert.assertNextRevision(2);
    }

    @Test
    public void addDuplicateNode() throws Exception {
        // Add /a and /a/b
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        Commit commit = new CommitImpl("/", "+a : { \"b\" : {} }", "Add /a, /a/b", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        command.execute();

        // Add /a/b again
        instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        commit = new CommitImpl("/a", "+b", "Add /a/b", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {
        }
    }

    @Test
    public void addNodesAndPropertiesOutOfOrder() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddPropertyInstructionImpl("/a", "key1", "value1"));
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        instructions.add(new AddPropertyInstructionImpl("/a/b", "key2", "value2"));
        instructions.add(new AddPropertyInstructionImpl("/a/c", "key3", "value3"));
        instructions.add(new AddNodeInstructionImpl("/a", "c"));

        Commit commit = new CommitImpl("/",
                "+a : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"key1\" : \"value1\", \"b#%1$s\" : { \"key2\" : \"value2\" } , \"c#%1$s\" : { \"key3\" : \"value3\" } } } }",
                                        revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertHeadRevision(1);
        MongoAssert.assertNextRevision(2);
    }

    @Test
    public void addNodesWhichAlreadyExist() throws Exception {
        SimpleNodeScenario scenario1 = new SimpleNodeScenario(mongoConnection);
        scenario1.create();

        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key1", "value1"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        instructions.add(new AddPropertyInstructionImpl("/a/b", "key2", "value2"));
        instructions.add(new AddNodeInstructionImpl("/a", "c"));
        instructions.add(new AddPropertyInstructionImpl("/a/c", "key3", "value3"));

        Commit commit = new CommitImpl("/",
                "+a : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%1$s\" : {  \"int\" : 1 , \"key1\" : \"value1\", \"b#%1$s\" : { \"string\" : \"foo\" , \"key2\" : \"value2\" } , \"c#%1$s\" : { \"bool\" : true , \"key3\" : \"value3\" } } } }",
                                        revisionId)));

        MongoAssert.assertCommitExists(commit);
        // MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/a", "/a/b", "/a/c"); TODO think about
        // whether / should really be included since it already contained /a
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/", "/a", "/a/b", "/a/c");
    }

    @Test
    public void commitAndMergeNodes() throws Exception {
        SimpleNodeScenario scenario1 = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario1.create();
        String secondRevisionId = scenario1.update_A_and_add_D_and_E();

        SimpleNodeScenario scenario2 = new SimpleNodeScenario(mongoConnection);
        String thirdRevisionId = scenario2.create();

        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"int\" : 1 , \"b#%1$s\" : { \"string\" : \"foo\" } , \"c#%1$s\" : { \"bool\" : true } } } }",
                                        firstRevisionId)));
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%2$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%2$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%1$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                        firstRevisionId, secondRevisionId)));
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%3$s\" : { \"a#%3$s\" : { \"int\" : 1 , \"double\" : 0.123 , \"b#%3$s\" : { \"string\" : \"foo\" , \"e#%2$s\" : { \"array\" : [ 123, null, 123.456, \"for:bar\", true ] } } , \"c#%3$s\" : { \"bool\" : true }, \"d#%2$s\" : { \"null\" : null } } } }",
                                        firstRevisionId, secondRevisionId,
                                        thirdRevisionId)));
    }

    @Test
    public void commitContainsAllAffectedNodes() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        String firstRevisionId = scenario.create();
        String secondRevisionId = scenario.update_A_and_add_D_and_E();

        MongoAssert.assertCommitContainsAffectedPaths(firstRevisionId, "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertCommitContainsAffectedPaths(secondRevisionId, "/a", "/a/b", "/a/d", "/a/b/e");
    }

    @Test
    public void removeNode() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));
        instructions.add(new AddNodeInstructionImpl("/a", "c"));

        Commit commit = new CommitImpl("/", "+a : { b : {} , c : {} }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();
        Assert.assertNotNull(revisionId);

        instructions = new LinkedList<Instruction>();
        instructions.add(new RemoveNodeInstructionImpl("/", "a"));

        commit = new CommitImpl("/", "-a", "This is a simple commit", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        revisionId = command.execute();
        Assert.assertNotNull(revisionId);

        MongoAssert.assertNodesExist("",
                NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(commit.getRevisionId(), "/");
    }

    @Test
    public void removeNonExistentNode() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/a", "b"));

        Commit commit = new CommitImpl("/", "+a : { b : {}  }", "Add nodes", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        command.execute();

        instructions = new LinkedList<Instruction>();
        instructions.add(new RemoveNodeInstructionImpl("/a", "c"));

        commit = new CommitImpl("/a", "-c", "Non-existent node delete", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {

        }
    }

    @Test
    public void existingParentContainsChildren() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/", "b"));
        instructions.add(new AddNodeInstructionImpl("/", "c"));

        Commit commit = new CommitImpl("/", "+a : { b : {} , c : {} }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert.assertNodesExist("", NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : {}, \"b#%1$s\" : {} , \"c#%1$s\" : {} } }", revisionId)));

        GetNodesCommandMongo command2 = new GetNodesCommandMongo(mongoConnection, "/", revisionId, 0);
        Node rootOfPath = command2.execute();
        Assert.assertEquals(3, rootOfPath.getChildCount());
    }

    @Test
    public void mergePropertiesAndChildren_noneExistedAndNewAdded() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key1", "value1"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key2", "value2"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key3", "value3"));

        Commit commit = new CommitImpl("/",
                "+a : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        MongoAssert.assertNodesExist("", NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", "0")));
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"key1\" : \"value1\", \"key2\" : \"value2\", \"key3\" : \"value3\" } } }",
                                        revisionId)));
    }

    @Test
    public void mergePropertiesAndChildren_someExistedAndNewAdded() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddPropertyInstructionImpl("/a", "existed_key1", "value1"));
        instructions.add(new AddPropertyInstructionImpl("/a", "existed_key2", "value2"));
        instructions.add(new AddPropertyInstructionImpl("/a", "existed_key3", "value3"));

        Commit commit = new CommitImpl("/",
                "+a : { \"existed_key1\" : \"value1\" , \"existed_key2\" : \"value2\" , \"existed_key3\" : \"value3\" }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();

        instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key1", "value1"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key2", "value2"));
        instructions.add(new AddPropertyInstructionImpl("/a", "key3", "value3"));

        commit = new CommitImpl("/",
                "+a : { \"key1\" : \"value1\" , \"key2\" : \"value2\" , \"key3\" : \"value3\" }",
                "This is a simple commit", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        revisionId = command.execute();

        MongoAssert.assertNodesExist("", NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", "0")));
        MongoAssert
                .assertNodesExist(
                        "",
                        NodeBuilder.build(String
                                .format("{ \"/#%1$s\" : { \"a#%1$s\" : { \"existed_key1\" : \"value1\", \"existed_key2\" : \"value2\", \"existed_key3\" : \"value3\", \"key1\" : \"value1\", \"key2\" : \"value2\", \"key3\" : \"value3\" } } }",
                                        revisionId)));
    }

    @Test
    public void noOtherNodesTouched() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/", "a"));
        instructions.add(new AddNodeInstructionImpl("/", "b"));
        instructions.add(new AddNodeInstructionImpl("/", "c"));

        Commit commit = new CommitImpl("/", "+a : { b : {} , c : {} }",
                "This is a simple commit", instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String firstRevisionId = command.execute();

        instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("/a", "d"));
        instructions.add(new AddNodeInstructionImpl("/a", "e"));

        commit = new CommitImpl("/a", "+d: {} \n+e : {}", "This is a simple commit", instructions);
        command = new CommitCommandMongo(mongoConnection, commit);
        String secondRevisionId = command.execute();

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
    public void rootNodeHasEmptyRootPath() throws Exception {
        List<Instruction> instructions = new LinkedList<Instruction>();
        instructions.add(new AddNodeInstructionImpl("", "/"));

        Commit commit = new CommitImpl(MongoUtil.INITIAL_COMMIT_PATH, MongoUtil.INITIAL_COMMIT_DIFF,
                MongoUtil.INITIAL_COMMIT_MESSAGE, instructions);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        String revisionId = command.execute();
        Assert.assertNotNull(revisionId);

        Node expected = NodeBuilder.build(String.format("{ \"/#%1$s\" : {} }", revisionId));
        MongoAssert.assertNodesExist(MongoUtil.INITIAL_COMMIT_PATH, expected);
    }

    @Test
    @Ignore
    // FIXME - This currently fails due to some limit in property sizes in Mongo
    // which affects path property.
    public void bigCommit() throws Exception {
        String path = "/";
        String baseNodeName = "test";
        int numberOfCommits = 1000;

        List<Instruction> instructions = new LinkedList<Instruction>();
        for (int i = 0; i < numberOfCommits; i++) {
            instructions.clear();
            instructions.add(new AddNodeInstructionImpl(path, baseNodeName + i));
            Commit commit = new CommitImpl(path, "+" + baseNodeName + i + " : {}",
                    "Add node n" + i, instructions);
            CommitCommandMongo command = new CommitCommandMongo(
                    mongoConnection, commit);
            command.execute();
            if (!PathUtils.denotesRoot(path)) {
                path += "/";
            }
            path += baseNodeName + i;
        }
    }
}
