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

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.MongoAssert;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.builder.NodeBuilder;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.scenario.SimpleNodeScenario;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@code CommitCommandMongo}
 */
public class CommitCommandMongoTest extends BaseMongoTest {

    @Test
    public void initialCommit() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {} , \"c\" : {} }", null);
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
    public void commitContainsAllAffectedNodes() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mongoConnection);
        Long firstRevisionId = scenario.create();
        Long secondRevisionId = scenario.update_A_and_add_D_and_E();
        MongoAssert.assertCommitContainsAffectedPaths(firstRevisionId, "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertCommitContainsAffectedPaths(secondRevisionId, "/a", "/a/b", "/a/d", "/a/b/e");
    }

    @Test
    public void noOtherNodesTouched() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : {}"
                + "+\"b\" : {}"
                + "+\"c\" : {}", null);
        CommitCommandMongo command = new CommitCommandMongo(mongoConnection, commit);
        Long firstRevisionId = command.execute();

        commit = CommitBuilder.build("/a", "+\"d\": {} +\"e\" : {}", null);
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
}