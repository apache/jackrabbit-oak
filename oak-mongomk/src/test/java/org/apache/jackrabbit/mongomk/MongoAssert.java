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
package org.apache.jackrabbit.mongomk;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.HeadMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.junit.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * Assertion utilities for {@code MongoDB} tests.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class MongoAssert {

    private static MongoConnection mongoConnection;

    public static void assertCommitContainsAffectedPaths(String revisionId, String... expectedPaths) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(revisionId)).get();
        CommitMongo result = (CommitMongo) commitCollection.findOne(query);
        Assert.assertNotNull(result);

        List<String> actualPaths = result.getAffectedPaths();
        Assert.assertEquals(new HashSet<String>(Arrays.asList(expectedPaths)), new HashSet<String>(actualPaths));
    }

    public static void assertCommitExists(Commit commit) {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        DBObject query = QueryBuilder.start(CommitMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(commit.getRevisionId())).and(CommitMongo.KEY_MESSAGE)
                .is(commit.getMessage()).and(CommitMongo.KEY_DIFF).is(commit.getDiff()).and(CommitMongo.KEY_PATH)
                .is(commit.getPath()).and(CommitMongo.KEY_FAILED).notEquals(Boolean.TRUE).get();
        CommitMongo result = (CommitMongo) commitCollection.findOne(query);
        Assert.assertNotNull(result);
    }

    public static void assertHeadRevision(long revisionId) {
        DBCollection headCollection = mongoConnection.getHeadCollection();
        HeadMongo result = (HeadMongo) headCollection.findOne();
        Assert.assertEquals(revisionId, result.getHeadRevisionId());
    }

    public static void assertNextRevision(long revisionId) {
        DBCollection headCollection = mongoConnection.getHeadCollection();
        HeadMongo result = (HeadMongo) headCollection.findOne();
        Assert.assertEquals(revisionId, result.getNextRevisionId());
    }

    public static void assertNodeRevisionId(String path, String revisionId, boolean exists) {
        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        DBObject query = QueryBuilder.start(NodeMongo.KEY_PATH).is(path).and(NodeMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(revisionId)).get();
        NodeMongo nodeMongo = (NodeMongo) nodeCollection.findOne(query);

        if (exists) {
            Assert.assertNotNull(nodeMongo);
        } else {
            Assert.assertNull(nodeMongo);
        }
    }

    public static void assertNodesExist(String parentPath, Node expected) {
        DBCollection nodeCollection = mongoConnection.getNodeCollection();
        QueryBuilder qb = QueryBuilder.start(NodeMongo.KEY_PATH).is(expected.getPath()).and(NodeMongo.KEY_REVISION_ID)
                .is(MongoUtil.toMongoRepresentation(expected.getRevisionId()));
        Map<String, Object> properties = expected.getProperties();
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                qb.and(NodeMongo.KEY_PROPERTIES + "." + entry.getKey()).is(entry.getValue());
            }
        }

        DBObject query = qb.get();

        NodeMongo nodeMongo = (NodeMongo) nodeCollection.findOne(query);
        Assert.assertNotNull(nodeMongo);

        Set<Node> children = expected.getChildren();
        if (children != null) {
            List<String> childNames = nodeMongo.getChildren();
            Assert.assertNotNull(childNames);
            Assert.assertEquals(children.size(), childNames.size());
            Assert.assertEquals(children.size(), new HashSet<String>(childNames).size());
            for (Node child : children) {
                assertNodesExist(expected.getPath(), child);
                Assert.assertTrue(childNames.contains(child.getName()));
            }
        } else {
            Assert.assertNull(nodeMongo.getChildren());
        }
    }

    static void setMongoConnection(MongoConnection mongoConnection) {
        // must be set prior to using this class.
        MongoAssert.mongoConnection = mongoConnection;
    }

    private MongoAssert() {
        // no instantiation
    }
}
