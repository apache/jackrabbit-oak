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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An action for fetching nodes.
 */
public class FetchNodesActionNew extends BaseAction<Map<String, MongoNode>> {

    /**
     * Trunk commits within this time frame are considered in doubt and are
     * checked more thoroughly whether they are valid.
     */
    private static final long IN_DOUBT_TIME_FRAME = 10000;

    public static final int LIMITLESS_DEPTH = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FetchNodesActionNew.class);

    private final Set<String> paths;
    private long revisionId;

    private String branchId;
    private int depth = LIMITLESS_DEPTH;

    /**
     * Maps valid commit revisionId to the baseRevId of the commit.
     */
    private final SortedMap<Long, Long> validCommits = new TreeMap<Long, Long>();

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
        checkArgument(revisionId >= 0, "revisionId must be >= 0");
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
        checkArgument(revisionId >= 0, "revisionId must be >= 0");
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
            String path = paths.iterator().next();
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
            String path = paths.iterator().next();
            if (depth == 0) {
                queryBuilder = queryBuilder.is(path);
            } else {
                Pattern pattern = createPrefixRegExp(path);
                queryBuilder = queryBuilder.regex(pattern);
            }
        }

        // FIXME - This needs to be improved to not fetch all revisions of a path.
        queryBuilder = queryBuilder.and(MongoNode.KEY_REVISION_ID).lessThanEquals(revisionId);

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

        // make sure we read from a valid commit
        MongoCommit commit = fetchCommit(revisionId);
        if (commit != null) {
            validCommits.put(revisionId, commit.getBaseRevisionId());
        } else {
            throw new MicroKernelException("Invalid revision: " + revisionId);
        }

        Map<String, MongoNode> nodes = new HashMap<String, MongoNode>();
        while (dbCursor.hasNext() && (numberOfNodesToFetch == -1 || nodes.size() < numberOfNodesToFetch)) {
            MongoNode node = (MongoNode)dbCursor.next();
            String path = node.getPath();
            // Assuming that revision ids are ordered and nodes are fetched in
            // sorted order, first check if the path is already in the map.
            if (nodes.containsKey(path)) {
                LOG.debug("Converted node @{} with path {} was not put into map"
                        + " because a newer version is available", node.getRevisionId(), path);
            } else {
                long revisionId = node.getRevisionId();
                if (isValid(node)) {
                    nodes.put(path, node);
                    LOG.debug("Converted node @{} with path {} was put into map", revisionId, path);
                } else {
                    LOG.debug("Node will not be converted as it is not part of a valid commit {} ({})",
                            path, revisionId);
                }
            }
        }
        dbCursor.close();
        return nodes;
    }

    /**
     * @param node the node to check.
     * @return <code>true</code> if the given node is from a valid commit;
     *         <code>false</code> otherwise.
     */
    private boolean isValid(@Nonnull MongoNode node) {
        long revisionId = node.getRevisionId();
        if (!validCommits.containsKey(revisionId) && nodeStore.getFromCache(revisionId) == null) {
            if (branchId == null) {
                // check if the given revisionId is a valid trunk commit
                return isValidTrunkCommit(revisionId);
            } else {
                // for branch commits we only check the failed flag and
                // assume there are no concurrent branch commits
                // FIXME: may need to revisit this
                MongoCommit commit = fetchCommit(revisionId);
                if (commit != null) {
                    validCommits.put(revisionId, commit.getBaseRevisionId());
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks if the given <code>revisionId</code> is from a valid trunk
     * commit.
     *
     * @param revisionId the commit revision.
     * @return whether the revisionId is valid.
     */
    private boolean isValidTrunkCommit(long revisionId) {
        if (validCommits.containsKey(revisionId)) {
            return true;
        }
        // if there is a lower valid revision than revisionId, we
        // know it is invalid
        if (!validCommits.headMap(revisionId).isEmpty()) {
            return false;
        }
        // at this point we know the validCommits does not go
        // back in history far enough to know if the revisionId is valid.
        // need to fetch base commit of oldest valid commit
        long inDoubt = System.currentTimeMillis() - IN_DOUBT_TIME_FRAME;
        MongoCommit commit;
        do {
            // base revision of the oldest known valid commit
            long baseRev = validCommits.values().iterator().next();
            commit = fetchCommit(baseRev);
            if (commit.getBaseRevisionId() != null) {
                validCommits.put(commit.getRevisionId(), commit.getBaseRevisionId());
            } else {
                // end of commit history
            }
            if (commit.getRevisionId() == revisionId) {
                return true;
            } else if (commit.getRevisionId() < revisionId) {
                // given revisionId is between two valid revisions -> invalid
                return false;
            }
        } while (commit.getTimestamp() > inDoubt);
        // revisionId is past in doubt time frame
        // perform simple check
        return fetchCommit(revisionId) != null;
    }

    /**
     * Fetches the commit with the given revisionId.
     *
     * @param revisionId the revisionId of a commit.
     * @return the commit or <code>null</code> if the commit does not exist or
     *         is marked as failed.
     */
    @CheckForNull
    private MongoCommit fetchCommit(long revisionId) {
        LOG.debug("Fetching commit @{}", revisionId);
        FetchCommitAction action = new FetchCommitAction(nodeStore, revisionId);
        try {
            return action.execute();
        } catch (Exception e) {
            // not a valid commit
        }
        return null;
    }
}