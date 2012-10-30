package org.apache.jackrabbit.mongomk.command;

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.api.command.DefaultCommand;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.query.FetchCommitsQuery;
import org.apache.jackrabbit.mongomk.query.FetchHeadRevisionIdQuery;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#getJournal(String, String, String)}
 */
public class GetJournalCommandMongo extends DefaultCommand<String> {

    private final String fromRevisionId;
    private final String toRevisionId;

    private String path;

    /**
     * Constructs a {@code GetJournalCommandMongo}
     *
     * @param mongoConnection Mongo connection.
     * @param fromRevisionId From revision.
     * @param toRevisionId To revision.
     * @param path Path.
     */
    public GetJournalCommandMongo(MongoConnection mongoConnection, String fromRevisionId,
            String toRevisionId, String path) {
        super(mongoConnection);
        this.fromRevisionId = fromRevisionId;
        this.toRevisionId = toRevisionId;
        this.path = path;
    }

    @Override
    public String execute() throws Exception {
        checkPath();
        boolean filtered = !"/".equals(path);

        long fromRevision = MongoUtil.toMongoRepresentation(fromRevisionId);
        long toRevision;
        if (toRevisionId == null) {
            FetchHeadRevisionIdQuery query = new FetchHeadRevisionIdQuery(mongoConnection);
            query.includeBranchCommits(true);
            toRevision = query.execute();
        } else {
            toRevision = MongoUtil.toMongoRepresentation(toRevisionId);
        }

        List<CommitMongo> commits = getCommits(fromRevision, toRevision);

        CommitMongo toCommit = extractCommit(commits, toRevision);
        if (toCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + toRevisionId);
        }

        CommitMongo fromCommit;
        if (toRevision == fromRevision) {
            fromCommit = toCommit;
        } else {
            fromCommit = extractCommit(commits, fromRevision);
            if (fromCommit == null || (fromCommit.getTimestamp() > toCommit.getTimestamp())) {
                // negative range, return empty journal
                return "[]";
            }
        }
        if (fromCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + fromRevisionId);
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // Iterate over commits in chronological order, starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            CommitMongo commit = commits.get(i);

            String diff = commit.getDiff();
            if (filtered) {
                try {
                    diff = new DiffBuilder(
                            MongoUtil.wrap(getNode("/", commit.getBaseRevId())),
                            MongoUtil.wrap(getNode("/", commit.getRevisionId())),
                            "/", -1, new MongoNodeStore(), path).build();
                    if (diff.isEmpty()) {
                        continue;
                    }
                } catch (Exception e) {
                    throw new MicroKernelException(e);
                }
            }
            commitBuff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage())
            .key("changes").value(diff).endObject();
        }
        return commitBuff.endArray().toString();
    }

    private void checkPath() {
        path = (path == null || path.isEmpty())? "/" : path;
    }

    private CommitMongo extractCommit(List<CommitMongo> commits, long revisionId) {
        for (CommitMongo commit : commits) {
            if (commit.getRevisionId() == revisionId) {
                return commit;
            }
        }
        return null;
    }

    private List<CommitMongo> getCommits(long fromRevisionId, long toRevisionId) {
        FetchCommitsQuery query = new FetchCommitsQuery(mongoConnection, fromRevisionId,
                toRevisionId);
        return query.execute();
    }

    private Node getNode(String path, long revisionId) throws Exception {
        GetNodesCommandMongo command = new GetNodesCommandMongo(mongoConnection,
                path, revisionId);
        return command.execute();
    }
}