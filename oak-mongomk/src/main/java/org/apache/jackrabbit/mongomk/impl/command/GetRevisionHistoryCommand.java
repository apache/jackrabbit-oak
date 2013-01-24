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
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.tree.SimpleMongoNodeStore;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#getRevisionHistory(long, int, String)}
 */
public class GetRevisionHistoryCommand extends BaseCommand<String> {

    private final long since;

    private int maxEntries;
    private String path;

    /**
     * Constructs a {@code GetRevisionHistoryCommandMongo}
     *
     * @param nodeStore Node store.
     * @param since Timestamp (ms) of earliest revision to be returned
     * @param maxEntries maximum #entries to be returned; if < 0, no limit will be applied.
     * @param path optional path filter; if {@code null} or {@code ""} the
     * default ({@code "/"}) will be assumed, i.e. no filter will be applied
     */
    public GetRevisionHistoryCommand(MongoNodeStore nodeStore,
            long since, int maxEntries, String path) {
        super(nodeStore);
        this.since = since;
        this.maxEntries = maxEntries;
        this.path = path;
    }

    @Override
    public String execute() {
        path = MongoUtil.adjustPath(path);
        maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;

        FetchCommitsAction action = new FetchCommitsAction(nodeStore);
        action.setMaxEntries(maxEntries);
        action.includeBranchCommits(false);

        List<MongoCommit> commits = action.execute();
        List<MongoCommit> history = new ArrayList<MongoCommit>();
        for (int i = commits.size() - 1; i >= 0; i--) {
            MongoCommit commit = commits.get(i);
            if (commit.getTimestamp() >= since) {
                if (MongoUtil.isFiltered(path)) {
                    try {
                        String diff = new DiffBuilder(
                                MongoUtil.wrap(getNode("/", commit.getBaseRevisionId())),
                                MongoUtil.wrap(getNode("/", commit.getRevisionId())),
                                "/", -1, new SimpleMongoNodeStore(), path).build();
                        if (!diff.isEmpty()) {
                            history.add(commit);
                        }
                    } catch (Exception e) {
                        throw new MicroKernelException(e);
                    }
                } else {
                    history.add(commit);
                }
            }
        }

        JsopBuilder buff = new JsopBuilder().array();
        for (MongoCommit commit : history) {
            buff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage())
            .endObject();
        }
        return buff.endArray().toString();
    }

    private Node getNode(String path, long revisionId) throws Exception {
        return new GetNodesCommandNew(nodeStore, path, revisionId).execute();
    }
}