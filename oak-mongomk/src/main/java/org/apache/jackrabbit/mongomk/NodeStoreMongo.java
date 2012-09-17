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
package org.apache.jackrabbit.mongomk;

import java.util.List;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.command.CommitCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetHeadRevisionCommandMongo;
import org.apache.jackrabbit.mongomk.command.GetNodesCommandMongo;
import org.apache.jackrabbit.mongomk.command.NodeExistsCommandMongo;
import org.apache.jackrabbit.mongomk.impl.command.CommandExecutorImpl;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.HeadMongo;
import org.apache.jackrabbit.mongomk.query.FetchValidCommitsQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

import com.mongodb.DBCollection;

/**
 * Implementation of {@link NodeStore} for the {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class NodeStoreMongo implements NodeStore {

    private static final long WAIT_FOR_COMMIT_POLL_MILLIS = 1000;

    private final CommandExecutor commandExecutor;
    private final MongoConnection mongoConnection;

    /**
     * Constructs a new {@code NodeStoreMongo}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     */
    public NodeStoreMongo(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
        commandExecutor = new CommandExecutorImpl();
    }

    @Override
    public String commit(Commit commit) throws Exception {
        Command<String> command = new CommitCommandMongo(mongoConnection, commit);

        return commandExecutor.execute(command);
    }

    @Override
    public String getHeadRevision() throws Exception {
        Command<String> command = new GetHeadRevisionCommandMongo(mongoConnection);

        return commandExecutor.execute(command);
    }

    @Override
    public Node getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws Exception {
        Command<Node> command = new GetNodesCommandMongo(mongoConnection, path, revisionId, depth);
        return commandExecutor.execute(command);
    }

    @Override
    public boolean nodeExists(String path, String revId) throws Exception {
        Command<Boolean> command = new NodeExistsCommandMongo(mongoConnection, path, revId);

        return commandExecutor.execute(command);
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId, String path) {
        path = (path == null || "".equals(path)) ? "/" : path;
        boolean filtered = !"/".equals(path);

        // FIXME [Mete] There's more work here.

        if (toRevisionId == null) {
            try {
                toRevisionId = new GetHeadRevisionCommandMongo(mongoConnection).execute();
            } catch (Exception e) {
                // FIXME Handle
            }
        }

        List<CommitMongo> commits = new FetchValidCommitsQuery(mongoConnection,
                fromRevisionId, toRevisionId).execute();

        CommitMongo toCommit = getCommit(commits, toRevisionId);

        CommitMongo fromCommit;
        if (toRevisionId.equals(fromRevisionId)) {
            fromCommit = toCommit;
        } else {
            fromCommit = getCommit(commits, fromRevisionId);
            if (fromCommit == null || (fromCommit.getTimestamp() > toCommit.getTimestamp())) {
                // negative range, return empty journal
                return "[]";
            }
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // iterate over commits in chronological order,
        // starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            CommitMongo commit = commits.get(i);
            //if (commit.getParentId() == null) {
            //   continue;
            //}
            String diff = commit.getDiff();
            // FIXME Check that filter really works.
            if (!filtered || commit.getAffectedPaths().contains(path)) {
                commitBuff.object()
                .key("id").value(String.valueOf(commit.getRevisionId()))
                .key("ts").value(commit.getTimestamp())
                .key("msg").value(commit.getMessage())
                .key("changes").value(diff).endObject();
            }
        }
        return commitBuff.endArray().toString();
    }

    private CommitMongo getCommit(List<CommitMongo> commits, String toRevisionId) {
        for (CommitMongo commit : commits) {
            if (String.valueOf(commit.getRevisionId()).equals(toRevisionId)) {
                return commit;
            }
        }
        return null;
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
      path = (path == null || "".equals(path)) ? "/" : path;
      boolean filtered = !"/".equals(path);
      maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;

      List<CommitMongo> history = new FetchValidCommitsQuery(mongoConnection, maxEntries).execute();
      JsopBuilder buff = new JsopBuilder().array();
      for (int i = history.size() - 1; i >= 0; i--) {
          CommitMongo commit = history.get(i);
          if (commit.getTimestamp() >= since) {
              // FIXME [Mete] Check that filter really works.
              if (!filtered || commit.getAffectedPaths().contains(path)) {
                  buff.object()
                  .key("id").value(String.valueOf(commit.getRevisionId()))
                  .key("ts").value(commit.getTimestamp())
                  .key("msg").value(commit.getMessage())
                  .endObject();
              }
          }
      }

      return buff.endArray().toString();
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws InterruptedException {
        long startTimestamp = System.currentTimeMillis();
        long initialHeadRevisionId = getHeadRevisionId();

        if (timeout <= 0) {
            return String.valueOf(initialHeadRevisionId);
        }

        long oldHeadRevision = MongoUtil.toMongoRepresentation(oldHeadRevisionId);
        if (oldHeadRevision < initialHeadRevisionId) {
            return String.valueOf(initialHeadRevisionId);
        }

        long waitForCommitPollMillis = Math.min(WAIT_FOR_COMMIT_POLL_MILLIS, timeout);
        while (true) {
            long headRevisionId = getHeadRevisionId();
            long now = System.currentTimeMillis();
            if (headRevisionId != initialHeadRevisionId || now - startTimestamp >= timeout) {
                return String.valueOf(headRevisionId);
            }
            Thread.sleep(waitForCommitPollMillis);
        }
    }

    private long getHeadRevisionId() {
        DBCollection headCollection = mongoConnection.getHeadCollection();
        HeadMongo headMongo = (HeadMongo)headCollection.findOne();
        long headRevisionId = headMongo.getHeadRevisionId();
        return headRevisionId;
    }
}
