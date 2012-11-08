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
package org.apache.jackrabbit.mongomk.perf;

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoSync;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MicroKernelPerfMaster {

    private class ContinousHandler extends DefaultJsopHandler {
        private final JSONObject jsonObject;

        private ContinousHandler() {
            this.jsonObject = new JSONObject();
        }

        @Override
        public void nodeAdded(String parentPath, String name) {
            try {
                if (!PathUtils.denotesRoot(name)) {
                    JSONObject parent = this.getObjectByPath(parentPath);
                    if (!parent.has(name)) {
                        parent.put(name, new JSONObject());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void propertySet(String path, String key, Object value) {
            try {
                if (!PathUtils.denotesRoot(key)) {
                    JSONObject element = this.getObjectByPath(path);
                    element.put(key, value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private JSONObject getObjectByPath(String path) throws Exception {
            JSONObject jsonObject = this.jsonObject;

            if (!"".equals(path)) {
                for (String segment : PathUtils.elements(path)) {
                    LOG.debug(String.format("Checking segment %s of path %s in object %s", segment, path, jsonObject));
                    jsonObject = jsonObject.optJSONObject(segment);
                    if (jsonObject == null) {
                        throw new Exception(String.format("The path %s was not found in the current state", path));
                    }
                }
            }

            return jsonObject;
        }
    }

    private static final Logger LOG = Logger.getLogger(MicroKernelPerfMaster.class);
    private final Config config;
    private ContinousHandler handler;
    private long lastCommitRevId;
    private long lastHeadRevId;
    private long lastRevId;
    private MongoMicroKernel microKernel;
    private MongoConnection mongoConnection;

    public MicroKernelPerfMaster(Config config) throws Exception {
        this.config = config;

        this.initMongo();
        this.initMicroKernel();
        this.initHandler();
    }

    public void start() throws Exception {
        LOG.info("Starting server...");

        this.startVerifying();
    }

    private void initHandler() {
        this.handler = new ContinousHandler();
    }

    private void initMicroKernel() throws Exception {
        NodeStore nodeStore = new MongoNodeStore(mongoConnection.getDB());
        BlobStore blobStore = new BlobStoreFS(System.getProperty("java.io.tmpdir"));
        microKernel = new MongoMicroKernel(mongoConnection, nodeStore, blobStore);
    }

    private void initMongo() throws Exception {
        this.mongoConnection = new MongoConnection(this.config.getMongoHost(), this.config.getMongoPort(),
                this.config.getMongoDatabase());
    }

    private void startVerifying() throws Exception {
        while (true) {
            List<MongoCommit> commitMongos = this.waitForCommit();
            for (MongoCommit commitMongo : commitMongos) {
                if (commitMongo.isFailed()) {
                    LOG.info(String.format("Skipping commit %d because it failed", commitMongo.getRevisionId()));
                    this.lastRevId = commitMongo.getRevisionId();
                } else {
                    LOG.info(String.format("Verifying commit %d", commitMongo.getRevisionId()));
                    this.verifyCommit(commitMongo);
                    this.verifyCommitOrder(commitMongo);
                    this.lastRevId = commitMongo.getRevisionId();
                    this.lastCommitRevId = commitMongo.getRevisionId();
                }
            }
        }
    }

    private void verifyCommit(MongoCommit commitMongo) throws Exception {
        String path = commitMongo.getPath();
        String jsop = commitMongo.getDiff();

        JsopParser jsopParser = new JsopParser(path, jsop, this.handler);
        jsopParser.parse();

        String json = this.microKernel.getNodes("/", String.valueOf(commitMongo.getRevisionId()), -1, 0, -1, null);
        JSONObject resultJson = new JSONObject(json);

        this.verifyEquality(this.handler.jsonObject, resultJson);

        LOG.info(String.format("Successfully verified commit %d", commitMongo.getRevisionId()));
    }

    private void verifyCommitOrder(MongoCommit commitMongo) throws Exception {
        long baseRevId = commitMongo.getBaseRevisionId();
        long revId = commitMongo.getRevisionId();
        if (baseRevId != this.lastCommitRevId) {
            throw new Exception(String.format(
                    "Revision %d has a base revision of %d but last successful commit was %d", revId, baseRevId,
                    this.lastCommitRevId));
        }
    }

    private void verifyEquality(JSONObject expected, JSONObject actual) throws Exception {
        LOG.debug(String.format("Verifying for equality %s (expected) vs %s (actual)", expected, actual));

        try {
            if (expected.length() != (actual.length() - 1)) { // substract 1 b/c of :childCount
                throw new Exception(String.format(
                        "Unequal number of children/properties: %d (expected) vs %d (actual)", expected.length(),
                        actual.length() - 1));
            }

            JSONArray expectedNames = expected.names();
            if (expectedNames != null) {
                for (int i = 0; i < expectedNames.length(); ++i) {
                    String name = expectedNames.getString(i);

                    Object expectedValue = expected.get(name);
                    Object actualValue = actual.get(name);

                    if ((expectedValue instanceof JSONObject) && (actualValue instanceof JSONObject)) {
                        this.verifyEquality((JSONObject) expectedValue, (JSONObject) actualValue);
                    } else if ((expectedValue != null) && (actualValue != null)) {
                        if (!expectedValue.equals(actualValue)) {
                            throw new Exception(String.format(
                                    "Key %s: Expected value '%s' does not macht actual value '%s'", name,
                                    expectedValue, actualValue));
                        }
                    } else if (expectedValue != null) {
                        throw new Exception(String.format(
                                "Key %s: Did not find an actual value for expected value '%s'", name, expectedValue));
                    } else if (actualValue != null) {
                        throw new Exception(String.format(
                                "Key %s: Did not find an expected value for actual value '%s'", name, actualValue));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(
                    String.format("Verificytion for equality failed: %s (expected) vs %s (actual)", expected, actual),
                    e);
            throw e;
        }
    }

    private List<MongoCommit> waitForCommit() {
        // TODO Change this to MicroKernel#waitForCommit
        List<MongoCommit> commitMongos = new LinkedList<MongoCommit>();
        this.lastHeadRevId = 0L;

        while (true) {
            LOG.debug("Waiting for commit...");

            DBCollection headCollection = ((MongoNodeStore)microKernel.getNodeStore()).getSyncCollection();
            MongoSync syncMongo = (MongoSync) headCollection.findOne();
            if (this.lastHeadRevId < syncMongo.getHeadRevisionId()) {
                DBCollection commitCollection = ((MongoNodeStore)microKernel.getNodeStore()).getCommitCollection();
                DBObject query = QueryBuilder.start(MongoCommit.KEY_REVISION_ID).greaterThan(this.lastRevId)
                        .and(MongoCommit.KEY_REVISION_ID).lessThanEquals(syncMongo.getHeadRevisionId()).get();
                DBObject sort = QueryBuilder.start(MongoCommit.KEY_REVISION_ID).is(1).get();
                DBCursor dbCursor = commitCollection.find(query).sort(sort);
                while (dbCursor.hasNext()) {
                    commitMongos.add((MongoCommit) dbCursor.next());
                }

                if (commitMongos.size() > 0) {
                    LOG.debug(String.format("Found %d new commits", commitMongos.size()));

                    break;
                }
                this.lastHeadRevId = syncMongo.getHeadRevisionId();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // noop
            }
        }

        return commitMongos;
    }
}
