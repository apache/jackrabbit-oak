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
package org.apache.jackrabbit.mongomk.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCursor;

/**
 * Utility class for queries.
 */
public class QueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(QueryUtils.class);

    /**
     * FIXME - This needs to take branchId into account. That is two paths with
     * different branch ids should be somehow preserved.
     *
     * Reads nodes from the given {@link DBCursor} and adds them to the returned
     * list if their revision id is contained in the list of given valid commits.
     * If multiple nodes with the same path are read by the cursor the one with
     * the highest revision id will win.
     *
     * @param dbCursor The {@code DBCursor} to read from.
     * @param validCommits The list of valid commits.
     * @return The list containing the valid nodes.
     */
    public static List<NodeMongo> getMostRecentValidNodes(DBCursor dbCursor,
            List<CommitMongo> validCommits) {
        List<Long> validRevisions = extractRevisionIds(validCommits);
        Map<String, NodeMongo> nodeMongos = new HashMap<String, NodeMongo>();

        while (dbCursor.hasNext()) {
            NodeMongo nodeMongo = (NodeMongo) dbCursor.next();

            String path = nodeMongo.getPath();
            long revisionId = nodeMongo.getRevisionId();

            LOG.debug(String.format("Converting node %s (%d)", path, revisionId));

            if (!validRevisions.contains(revisionId)) {
                LOG.debug(String.format("Node will not be converted b/c it is not a valid commit %s (%d)", path, revisionId));
                continue;
            }

            NodeMongo existingNodeMongo = nodeMongos.get(path);
            if (existingNodeMongo != null) {
                long existingRevId = existingNodeMongo.getRevisionId();

                if (revisionId > existingRevId) {
                    nodeMongos.put(path, nodeMongo);
                    LOG.debug(String.format("Converted nodes was put into map and replaced %s (%d)", path, revisionId));
                } else {
                    LOG.debug(String.format("Converted nodes was not put into map because a newer version"
                            + " is available %s (%d)", path, revisionId));
                }
            } else {
                nodeMongos.put(path, nodeMongo);
                LOG.debug("Converted node was put into map");
            }
        }

        return new ArrayList<NodeMongo>(nodeMongos.values());
    }

    private static List<Long> extractRevisionIds(List<CommitMongo> validCommits) {
        List<Long> validRevisions = new ArrayList<Long>(validCommits.size());
        for (CommitMongo commitMongo : validCommits) {
            validRevisions.add(commitMongo.getRevisionId());
        }
        return validRevisions;
    }
}
