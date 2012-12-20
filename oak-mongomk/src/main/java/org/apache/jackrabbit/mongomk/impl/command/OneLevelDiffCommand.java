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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchNodesActionNew;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;
import org.apache.jackrabbit.mongomk.impl.model.NodeImpl;
import org.apache.jackrabbit.mongomk.impl.model.tree.SimpleMongoNodeStore;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>OneLevelDiffCommand</code> implements a specialized {@link DiffCommand}
 * with a fixed depth of 0.
 */
public class OneLevelDiffCommand extends BaseCommand<String> {

    private final long fromRevision;
    private final long toRevision;
    private final String path;
    private final int pathDepth;

    public OneLevelDiffCommand(MongoNodeStore nodeStore, String fromRevision,
                       String toRevision, String path) throws Exception {
        super(nodeStore);
        this.fromRevision = MongoUtil.toMongoRepresentation(checkNotNull(fromRevision));
        this.toRevision = MongoUtil.toMongoRepresentation(checkNotNull(toRevision));
        this.path = MongoUtil.adjustPath(path);
        this.pathDepth = PathUtils.getDepth(this.path);
    }

    @Override
    public String execute() throws Exception {
        MongoCommit fromCommit = new FetchCommitAction(
                nodeStore, fromRevision).execute();
        MongoCommit toCommit = new FetchCommitAction(
                nodeStore, toRevision).execute();
        FetchNodesActionNew action = new FetchNodesActionNew(
                nodeStore, path, 0, fromRevision);
        action.setBranchId(fromCommit.getBranchId());
        NodeImpl fromNode = MongoNode.toNode(action.execute().get(path));
        action = new FetchNodesActionNew(
                nodeStore, path, 0, toRevision);
        action.setBranchId(toCommit.getBranchId());
        NodeImpl toNode = MongoNode.toNode(action.execute().get(path));

        String diff = "";
        if (!fromNode.getRevisionId().equals(toNode.getRevisionId())) {
            // diff of node at given path
            DiffBuilder diffBuilder = new DiffBuilder(MongoUtil.wrap(fromNode),
                    MongoUtil.wrap(toNode), path, 0, new SimpleMongoNodeStore(), path);
            diff = diffBuilder.build();
        }

        // find out what changed below path
        List<MongoCommit> commits = getCommits(fromCommit, toCommit);
        Set<String> affectedPaths = new HashSet<String>();
        for (MongoCommit mc : commits) {
            for (String p : mc.getAffectedPaths()) {
                if (p.startsWith(path)) {
                    int d = PathUtils.getDepth(p);
                    if (d > pathDepth) {
                        affectedPaths.add(PathUtils.getAncestorPath(p, d - pathDepth - 1));
                    }
                }
            }
        }

        JsopBuilder builder = new JsopBuilder();
        for (String p : affectedPaths) {
            builder.tag('^');
            builder.key(p);
            builder.object().endObject();
            builder.newline();
        }

        return diff + builder.toString();
    }

    /**
     * Retrieves the commits within the range of <code>c1</code> and
     * <code>c2</code>. The given bounding commits are included in the list as
     * well.
     *
     * @param c1 a MongoCommit
     * @param c2 a MongoCommit
     * @return the commits from <code>c1</code> to <code>c2</code>.
     * @throws Exception if an error occurs.
     */
    private List<MongoCommit> getCommits(MongoCommit c1, MongoCommit c2)
            throws Exception {
        // this implementation does not use the multi commit fetch action
        // FetchCommitsAction because that action does not leverage the
        // commit cache in NodeStore. Retrieving each commit individually
        // results in good cache hit ratios when the revision range is recent
        // and not too big.
        List<MongoCommit> commits = new ArrayList<MongoCommit>();
        MongoCommit fromCommit = c1.getRevisionId() < c2.getRevisionId() ? c1 : c2;
        MongoCommit toCommit = c1.getRevisionId() < c2.getRevisionId() ? c2 : c1;
        Long revision = toCommit.getBaseRevisionId();
        commits.add(toCommit);
        while (revision != null && revision > fromCommit.getRevisionId()) {
            MongoCommit c = new FetchCommitAction(nodeStore, revision).execute();
            commits.add(c);
            revision = c.getBaseRevisionId();
        }
        commits.add(fromCommit);
        return commits;
    }
}
