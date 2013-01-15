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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * FIXME - This is same as FetchNodesAction except that it does not require
 * the list of all valid commits upfront. It also has some optimizations on how
 * it fetches nodes. Consolidate the two.
 *
 * An action for fetching nodes.
 */
public class FetchNodesActionNew extends BaseAction<Map<String, MongoNode>> {

    public static final int LIMITLESS_DEPTH = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesActionNew.class);

    private final Set<String> paths;
    private long revisionId = -1;

    private String branchId;
    private int depth = LIMITLESS_DEPTH;

    /**
     * Constructs a new {@code FetchNodesAction} to fetch a node and optionally
     * its descendants under the specified path.
     *
     * @param nodeStore Node store.
     * @param path The path.
     * @param depth The depth.
     * @param revisionId The revision id.
     */
    public FetchNodesActionNew(MongoNodeStore nodeStore, String path, int depth,
            long revisionId) {
        super(nodeStore);
        paths = new HashSet<String>();
        paths.add(path);
        this.depth = depth;
        this.revisionId = revisionId;
    }

    /**
     * Constructs a new {@code FetchNodesAction} to fetch nodes with the exact
     * specified paths.
     *
     * @param nodeStore Node store.
     * @param paths The exact paths to fetch nodes for.
     * @param revisionId The revision id.
     */
    public FetchNodesActionNew(MongoNodeStore nodeStore, Set<String> paths, long revisionId) {
        super(nodeStore);
        this.paths = paths;
        this.revisionId = revisionId;
    }

    /**
     * Sets the branchId for the query.
     *
     * @param branchId Branch id.
     */
    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    @Override
    public Map<String, MongoNode> execute() {
        if (paths.isEmpty()) {
            return Collections.emptyMap();
        }

        // FIXME - Should deal with multiple paths as long as depth = 0
        if (paths.size() == 1 && depth == 0) {
            String path = paths.toArray(new String[0])[0];
            MongoNode node = nodeStore.getFromCache(path, branchId, revisionId);
            if (node != null) {
                Map<String, MongoNode> nodes = new HashMap<String, MongoNode>();
                nodes.put(node.getPath(), node);
                return nodes;
            }
        }

        DBCursor dbCursor = performQuery();
        Map<String, MongoNode> nodes = getMostRecentValidNodes(dbCursor);
        for (MongoNode node : nodes.values()) {
            nodeStore.cache(node);
        }
        return nodes;
    }

    private DBCursor performQuery() {
        QueryBuilder queryBuilder = QueryBuilder.start(MongoNode.KEY_PATH);
        if (paths.size() > 1) {
            queryBuilder = queryBuilder.in(paths);
        } else {
            String path = paths.toArray(new String[0])[0];
            if (depth == 0) {
                queryBuilder = queryBuilder.is(path);
            } else {
                Pattern pattern = createPrefixRegExp(path);
                queryBuilder = queryBuilder.regex(pattern);
            }
        }

        // FIXME - This needs to be improved to not fetch all revisions of a path.
        if (revisionId > -1) {
            queryBuilder = queryBuilder.and(MongoNode.KEY_REVISION_ID).lessThanEquals(revisionId);
        }

        if (branchId == null) {
            DBObject query = new BasicDBObject(MongoNode.KEY_BRANCH_ID, new BasicDBObject("$exists", false));
            queryBuilder = queryBuilder.and(query);
        } else {
            // Not only return nodes in the branch but also nodes in the trunk
            // before the branch was created.
            long headBranchRevisionId = Long.parseLong(branchId.substring(0, branchId.indexOf("-")));

            DBObject branchQuery = QueryBuilder.start().or(
                    QueryBuilder.start(MongoNode.KEY_BRANCH_ID).is(branchId).get(),
                    QueryBuilder.start().and(
                            QueryBuilder.start(MongoNode.KEY_REVISION_ID).lessThanEquals(headBranchRevisionId).get(),
                            new BasicDBObject(MongoNode.KEY_BRANCH_ID, new BasicDBObject("$exists", false))
                    ).get()
            ).get();
            queryBuilder = queryBuilder.and(branchQuery);
        }

        DBObject orderBy = new BasicDBObject();
        orderBy.put(MongoNode.KEY_PATH, 1);
        orderBy.put(MongoNode.KEY_REVISION_ID, -1);

        DBObject query = queryBuilder.get();
        LOG.debug("Executing query: {}", query);

        return nodeStore.getNodeCollection().find(query).sort(orderBy);
    }

    private Pattern createPrefixRegExp(String path) {
        StringBuilder regex = new StringBuilder();
        regex.append("^");
        if (!"/".equals(path)) {
            regex.append(Pattern.quote(path));
        }
        regex.append("(/[^/]*)");
        regex.append("{0,");
        if (depth > 0) {
            regex.append(depth);
        }
        regex.append("}$");

        return Pattern.compile(regex.toString());
    }

    private Map<String, MongoNode> getMostRecentValidNodes(DBCursor dbCursor) {
        int numberOfNodesToFetch = -1;
        if (paths.size() > 1) {
            numberOfNodesToFetch = paths.size();
        } else if (depth == 0) {
            numberOfNodesToFetch = 1;
        }

        Map<String, MongoNode> nodes = new HashMap<String, MongoNode>();
        Map<Long, MongoCommit> commits = new HashMap<Long, MongoCommit>();

        while (dbCursor.hasNext() && (numberOfNodesToFetch == -1 || nodes.size() < numberOfNodesToFetch)) {
            MongoNode node = (MongoNode)dbCursor.next();
            String path = node.getPath();
            // Assuming that revision ids are ordered and nodes are fetched in
            // sorted order, first check if the path is already in the map.
            if (nodes.containsKey(path)) {
                LOG.debug("Converted node @{} with path {} was not put into map"
                        + " because a newer version is available", node.getRevisionId(), path);
                continue;
            } else {
                long revisionId = node.getRevisionId();
                LOG.debug("Converting node {} (@{})", path, revisionId);

                if (!commits.containsKey(revisionId) && nodeStore.getFromCache(revisionId) == null) {
                    LOG.debug("Fetching commit @{}", revisionId);
                    FetchCommitAction action = new FetchCommitAction(nodeStore, revisionId);
                    try {
                        MongoCommit commit = action.execute();
                        commits.put(revisionId, commit);
                    } catch (Exception e) {
                        LOG.debug("Node will not be converted as it is not part of a valid commit {} ({})",
                                path, revisionId);
                        continue;
                    }
                }
                nodes.put(path, node);
                LOG.debug("Converted node @{} with path {} was put into map", revisionId, path);
            }

            // This is for unordered revision ids.
            /*
            MongoNode existingNodeMongo = nodeMongos.get(path);
            if (existingNodeMongo != null) {
                long existingRevId = existingNodeMongo.getRevisionId();

                if (revisionId > existingRevId) {
                    nodeMongos.put(path, nodeMongo);
                    LOG.debug("Converted nodes was put into map and replaced {} ({})", path, revisionId);
                } else {
                    LOG.debug("Converted nodes was not put into map because a newer version"
                            + " is available {} ({})", path, revisionId);
                }
            } else {
                nodeMongos.put(path, nodeMongo);
                LOG.debug("Converted node @{} with path {} was put into map", revisionId, path);
            }
            */
        }
        dbCursor.close();
        return nodes;
    }
}