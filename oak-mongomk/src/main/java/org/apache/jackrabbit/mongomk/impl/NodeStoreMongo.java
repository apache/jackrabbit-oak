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
package org.apache.jackrabbit.mongomk.impl;

import org.apache.jackrabbit.mongomk.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.command.CommitCommand;
import org.apache.jackrabbit.mongomk.impl.command.DefaultCommandExecutor;
import org.apache.jackrabbit.mongomk.impl.command.DiffCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetHeadRevisionCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetJournalCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetNodesCommand;
import org.apache.jackrabbit.mongomk.impl.command.GetRevisionHistoryCommand;
import org.apache.jackrabbit.mongomk.impl.command.MergeCommand;
import org.apache.jackrabbit.mongomk.impl.command.NodeExistsCommand;
import org.apache.jackrabbit.mongomk.impl.command.WaitForCommitCommand;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 */
public class NodeStoreMongo implements NodeStore {

    private final CommandExecutor commandExecutor;
    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code NodeStoreMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public NodeStoreMongo(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
        commandExecutor = new DefaultCommandExecutor();
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<Long> command = new CommitCommand(mongoConnection, commit);
        Long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public String diff(String fromRevision, String toRevision, String path, int depth)
            throws Exception {
        Command<String> command = new DiffCommand(mongoConnection,
                fromRevision, toRevision, path, depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String getHeadRevision() throws Exception {
        GetHeadRevisionCommand command = new GetHeadRevisionCommand(mongoConnection);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        GetNodesCommand command = new GetNodesCommand(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        command.setBranchId(getBranchId(revisionId));
        command.setDepth(depth);
        return commandExecutor.execute(command);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws Exception {
        MergeCommand command = new MergeCommand(mongoConnection,
                branchRevisionId, message);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws Exception {
        NodeExistsCommand command = new NodeExistsCommand(mongoConnection, path,
                MongoUtil.toMongoRepresentation(revisionId));
        String branchId = getBranchId(revisionId);
        command.setBranchId(branchId);
        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path)
            throws Exception {
        GetJournalCommand command = new GetJournalCommand(mongoConnection,
                fromRevisionId, toRevisionId, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws Exception {
        GetRevisionHistoryCommand command = new GetRevisionHistoryCommand(mongoConnection,
                since, maxEntries, path);
        return commandExecutor.execute(command);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws Exception {
        WaitForCommitCommand command = new WaitForCommitCommand(mongoConnection,
                oldHeadRevisionId, timeout);
        long revisionId = commandExecutor.execute(command);
        return MongoUtil.fromMongoRepresentation(revisionId);
    }

    private String getBranchId(String revisionId) throws Exception {
        if (revisionId == null) {
            return null;
        }

        CommitMongo baseCommit = new FetchCommitAction(mongoConnection,
                MongoUtil.toMongoRepresentation(revisionId)).execute();
        return baseCommit.getBranchId();
    }
}