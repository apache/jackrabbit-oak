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
package org.apache.jackrabbit.mongomk.impl.command;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.MongoAssert;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.impl.SimpleNodeScenario;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.mongomk.util.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@code CommitCommandMongo}
 */
public class CommitCommandTest extends BaseMongoMicroKernelTest {

    @Test
    public void initialCommit() throws Exception {
        Commit commit = CommitBuilder.build("/", "+\"a\" : { \"b\" : {} , \"c\" : {} }", null);
        CommitCommand command = new CommitCommand(getNodeStore(), commit);
        Long revisionId = command.execute();

        Assert.assertNotNull(revisionId);
        MongoAssert.assertNodesExist(NodeBuilder.build(String.format(
                "{ \"/#%1$s\" : { \"a#%1$s\" : { \"b#%1$s\" : {} , \"c#%1$s\" : {} } } }", revisionId)));

        MongoAssert.assertCommitExists(commit);
        MongoAssert.assertCommitContainsAffectedPaths(MongoUtil.fromMongoRepresentation(commit.getRevisionId()),
                "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertHeadRevision(1);
        MongoAssert.assertNextRevision(2);
    }

    @Test
    public void ontainsAllAffectedNodes() throws Exception {
        SimpleNodeScenario scenario = new SimpleNodeScenario(mk);
        String rev1 = scenario.create();
        String rev2 = scenario.update_A_and_add_D_and_E();
        MongoAssert.assertCommitContainsAffectedPaths(rev1, "/", "/a", "/a/b", "/a/c");
        MongoAssert.assertCommitContainsAffectedPaths(rev2, "/a", "/a/b", "/a/d", "/a/b/e");
    }

    @Test
    public void noOtherNodesTouched() throws Exception {
        String rev1 = mk.commit("/", "+\"a\" : {} +\"b\" : {} +\"c\" : {}", null, null);
        String rev2 = mk.commit("/a", "+\"d\": {} +\"e\" : {}", null, null);

        MongoAssert.assertNodeRevisionId("/", rev1, true);
        MongoAssert.assertNodeRevisionId("/a", rev1, true);
        MongoAssert.assertNodeRevisionId("/b", rev1, true);
        MongoAssert.assertNodeRevisionId("/c", rev1, true);
        MongoAssert.assertNodeRevisionId("/a/d", rev1, false);
        MongoAssert.assertNodeRevisionId("/a/e", rev1, false);

        MongoAssert.assertNodeRevisionId("/", rev2, false);
        MongoAssert.assertNodeRevisionId("/a", rev2, true);
        MongoAssert.assertNodeRevisionId("/b", rev2, false);
        MongoAssert.assertNodeRevisionId("/c", rev2, false);
        MongoAssert.assertNodeRevisionId("/a/d", rev2, true);
        MongoAssert.assertNodeRevisionId("/a/e", rev2, true);
    }
}