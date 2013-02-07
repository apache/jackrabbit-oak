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

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.mongomk.impl.json.NormalizingJsopHandler;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.tree.SimpleMongoNodeStore;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#getJournal(String, String, String)}
 */
public class GetJournalCommand extends BaseCommand<String> {

    private final String fromRevisionId;
    private final String toRevisionId;

    private String path;

    /**
     * Constructs a {@code GetJournalCommandMongo}
     *
     * @param nodeStore Node store.
     * @param fromRevisionId From revision.
     * @param toRevisionId To revision.
     * @param path Path.
     */
    public GetJournalCommand(MongoNodeStore nodeStore, String fromRevisionId,
            String toRevisionId, String path) {
        super(nodeStore);
        this.fromRevisionId = fromRevisionId;
        this.toRevisionId = toRevisionId;
        this.path = path;
    }

    @Override
    public String execute() throws Exception {
        path = MongoUtil.adjustPath(path);

        long fromRevision = MongoUtil.toMongoRepresentation(fromRevisionId);
        long toRevision;
        if (toRevisionId == null) {
            toRevision = new FetchHeadRevisionIdAction(nodeStore).execute();
        } else {
            toRevision = MongoUtil.toMongoRepresentation(toRevisionId);
        }

        List<MongoCommit> commits = getCommits(fromRevision, toRevision);

        MongoCommit toCommit = extractCommit(commits, toRevision);
        MongoCommit fromCommit;
        if (toRevision == fromRevision) {
            fromCommit = toCommit;
        } else {
            fromCommit = extractCommit(commits, fromRevision);
        }

        if (fromCommit != null && fromCommit.getBranchId() != null) {
            if (!fromCommit.getBranchId().equals(toCommit.getBranchId())) {
                throw new MicroKernelException("Inconsistent range specified: fromRevision denotes a private branch while toRevision denotes a head or another private branch");
            }
        }

        if (fromCommit != null && fromCommit.getTimestamp() > toCommit.getTimestamp()) {
            // negative range, return empty journal
            return "[]";
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // Iterate over commits in chronological order, starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            MongoCommit commit = commits.get(i);

            String diff = commit.getDiff();
            if (MongoUtil.isFiltered(path)) {
                try {
                    diff = new DiffBuilder(
                            MongoUtil.wrap(getNode("/", commit.getBaseRevisionId())),
                            MongoUtil.wrap(getNode("/", commit.getRevisionId())),
                            "/", -1, new SimpleMongoNodeStore(), path).build();
                    if (diff.isEmpty()) {
                        continue;
                    }
                } catch (Exception e) {
                    throw new MicroKernelException(e);
                }
            } else {
                diff = normalizeDiff(commit.getPath(), diff);
            }

            commitBuff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage());
            if (commit.getBranchId() != null) {
                commitBuff.key("branchRootId").value(commit.getBranchId().toString());
            }
            commitBuff.key("changes").value(diff).endObject();
        }
        return commitBuff.endArray().toString();
    }

    private MongoCommit extractCommit(List<MongoCommit> commits, long revisionId) {
        for (MongoCommit commit : commits) {
            if (commit.getRevisionId() == revisionId) {
                return commit;
            }
        }
        return null;
    }

    private List<MongoCommit> getCommits(long fromRevisionId, long toRevisionId) {
        return new FetchCommitsAction(nodeStore, fromRevisionId, toRevisionId).execute();
    }

    private Node getNode(String path, long revisionId) throws Exception {
        return new GetNodesCommand(nodeStore, path, revisionId).execute();
    }

    private String normalizeDiff(String path, String diff) throws Exception {
        // Need to normalize against empty path in journal diffs.
        NormalizingJsopHandler handler = new NormalizingJsopHandler("");
        new JsopParser(path, diff, handler).parse();
        return handler.getDiff();
    }
}