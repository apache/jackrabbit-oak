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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mongomk.MongoConnection;
import org.apache.jackrabbit.mongomk.NodeStoreMongo;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.mongomk.perf.RandomJsopGenerator.RandomJsop;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class MicroKernelPerfClient {

    private static class Stats extends BasicDBObject {
        private static final long serialVersionUID = -8819985408570757782L;

        private Stats(String id, double duration, long numOfCommits, long numOfNodes, long numOfObjects) {
            put("id", id);
            put("duration", duration);
            put("numOfCommits", numOfCommits);
            put("numOfNodes", numOfNodes);
            put("numOfObjects", numOfObjects);
        }
    }

    private static class VerificationHandler extends DefaultJsopHandler {

        private final List<String> addedNodes = new LinkedList<String>();
        private final Map<String, List<String>> addedProperties = new HashMap<String, List<String>>();

        @Override
        public void nodeAdded(String parentPath, String name) {
            addedNodes.add(PathUtils.concat(parentPath, name));
        }

        @Override
        public void propertyAdded(String path, String key, Object value) {
            List<String> properties = addedProperties.get(path);
            if (properties == null) {
                properties = new LinkedList<String>();
                addedProperties.put(path, properties);
            }
            properties.add(key);
        }
    }

    private static final Logger LOG = Logger.getLogger(MicroKernelPerfClient.class);

    private static final Logger PERF = Logger.getLogger("PERFORMANCE");

    private static void assertNodeExists(String path, String node, JSONObject result) throws Exception {
        if (!path.equals(node)) {
            JSONObject temp = result;
            for (String segment : PathUtils.elements(node)) {
                temp = temp.getJSONObject(segment);

                if (temp == null) {
                    throw new Exception(String.format("The node %s could not be found!", node));
                }
            }
        }
    }

    private static void assertPropertyExists(String path, String property, JSONObject result) throws Exception {
        JSONObject temp = result;
        for (String segment : PathUtils.elements(path)) {
            temp = temp.optJSONObject(segment);

            if (temp == null) {
                throw new Exception(String.format("The node %s could not be found!", path));
            }
        }

        Object value = temp.opt(property);
        if (value == null) {
            throw new Exception(String.format("The node %s did not containt the property %s!", path, property));
        }
    }

    private Monitor commitMonitor;
    private final Config config;
    private Monitor getNodesMonitor;

    private MongoMicroKernel microKernel;

    private MongoConnection mongoConnection;

    private RandomJsopGenerator randomJsopGenerator;

    public MicroKernelPerfClient(Config config) throws Exception {
        this.config = config;

        initMongo();
        initMicroKernel();
        initRandomJsopGenerator();
        initMonitoring();
    }

    public void start() throws Exception {
        LOG.info("Starting client...");

        startCommitting();
    }

    private void createStats(VerificationHandler handler, JSONObject result) {
        long numOfNodes = mongoConnection.getNodeCollection().count();
        long numOfCommits = mongoConnection.getCommitCollection().count();

        Stats commitStats = new Stats("commit", commitMonitor.getLastValue(), numOfCommits, numOfNodes,
                handler.addedNodes.size() + handler.addedProperties.size());

        Stats getNodesStats = new Stats("getNodes", getNodesMonitor.getLastValue(), numOfCommits, numOfNodes,
                numOfNodes - handler.addedNodes.size());

        DBCollection statsCollection = mongoConnection.getDB().getCollection("statistics");
        statsCollection.insert(new Stats[] { commitStats, getNodesStats }, WriteConcern.NONE);
    }

    private void initMicroKernel() throws Exception {
        NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
        BlobStore blobStore = new BlobStoreFS(System.getProperty("java.io.tmpdir"));

        microKernel = new MongoMicroKernel(nodeStore, blobStore);
    }

    private void initMongo() throws Exception {
        mongoConnection = new MongoConnection(config.getMongoHost(), config.getMongoPort(), config.getMongoDatabase());
    }

    private void initMonitoring() {
        commitMonitor = MonitorFactory.getTimeMonitor("commit");
        getNodesMonitor = MonitorFactory.getTimeMonitor("getNodes");
    }

    private void initRandomJsopGenerator() throws Exception {
        randomJsopGenerator = new RandomJsopGenerator();
    }

    private void startCommitting() throws Exception {
        while (true) {
            RandomJsop randomJsop = randomJsopGenerator.nextRandom();

            String commitPath = randomJsop.getPath();
            String jsonDiff = randomJsop.getJsop();
            String revisionId = null;
            String message = randomJsop.getMessage();

            commitMonitor.start();
            String newRevisionId = microKernel.commit(commitPath, jsonDiff, revisionId, message);
            commitMonitor.stop();
            PERF.info(commitMonitor);
            LOG.debug(String.format("Committed (%s): %s, %s\n%s", newRevisionId, commitPath, message, jsonDiff));

            getNodesMonitor.start();
            String getPath = "".equals(commitPath) ? "/" : commitPath;
            String json = microKernel.getNodes(getPath, newRevisionId, -1, 0, -1, null);
            getNodesMonitor.stop();
            PERF.info(getNodesMonitor);
            LOG.debug(String.format("GetNodes (%s: %s", newRevisionId, json));

            VerificationHandler handler = new VerificationHandler();
            JsopParser jsopParser = new JsopParser(commitPath, jsonDiff, handler);
            jsopParser.parse();

            JSONObject result = new JSONObject(json);

            verify(handler, result, getPath);
            createStats(handler, result);

            randomJsopGenerator.setSeed(getPath, json);
        }
    }

    private void verify(VerificationHandler handler, JSONObject result, String getPath) throws Exception {
        for (String node : handler.addedNodes) {
            assertNodeExists(getPath, node, result);
        }

        for (Map.Entry<String, List<String>> entry : handler.addedProperties.entrySet()) {
            String path = entry.getKey();
            List<String> properties = entry.getValue();
            for (String property : properties) {
                assertPropertyExists(path, property, result);
            }
        }
    }
}
