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

import org.apache.jackrabbit.mongomk.model.NodeMongo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCursor;

/**
 * Utility class for queries.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class QueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(QueryUtils.class);

    /**
     * Reads nodes from the given {@link DBCursor} and add them to the returned list if their revision id is contained
     * in the list of given valid revisions. If multiple nodes with the same path are read by the cursor the one with
     * the highest revision id will win.
     *
     * @param dbCursor
     *            The {@code DBCursor} to read from.
     * @param validRevisions
     *            The list of valid revisions.
     * @return The list containing the valid nodes.
     */
    static List<NodeMongo> convertToNodes(DBCursor dbCursor, List<Long> validRevisions) {
        Map<String, NodeMongo> nodeMongos = new HashMap<String, NodeMongo>();

        while (dbCursor.hasNext()) {
            NodeMongo nodeMongo = (NodeMongo) dbCursor.next();

            String path = nodeMongo.getPath();
            long revId = nodeMongo.getRevisionId();

            LOG.debug(String.format("Converting node %s (%d)", path, revId));

            if (!validRevisions.contains(revId)) {
                LOG.debug(String.format("Node will not be converted b/c it is not a valid commit %s (%d)", path, revId));

                continue;
            }

            NodeMongo existingNodeMongo = nodeMongos.get(path);
            if (existingNodeMongo != null) {
                long existingRevId = existingNodeMongo.getRevisionId();

                if (revId > existingRevId) {
                    nodeMongos.put(path, nodeMongo);
                    LOG.debug(String.format("Converted nodes was put into map and replaced %s (%d)", path, revId));
                } else {
                    LOG.debug(String.format(
                            "Converted nodes was not put into map because a newer version is available %s (%d)", path,
                            revId));
                }
            } else {
                nodeMongos.put(path, nodeMongo);
                LOG.debug("Converted node was put into map");
            }
        }

        return new ArrayList<NodeMongo>(nodeMongos.values());
    }

    private QueryUtils() {
        // no initialization
    }
}
