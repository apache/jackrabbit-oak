package org.apache.jackrabbit.mongomk.impl.command;

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#getJournal(String, String, String)}
 */
public class GetJournalCommand extends DefaultCommand<String> {

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
    public GetJournalCommand(MongoConnection mongoConnection, String fromRevisionId,
            String toRevisionId, String path) {
        super(mongoConnection);
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
            FetchHeadRevisionIdAction query = new FetchHeadRevisionIdAction(mongoConnection);
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
            if (MongoUtil.isFiltered(path)) {
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

    private CommitMongo extractCommit(List<CommitMongo> commits, long revisionId) {
        for (CommitMongo commit : commits) {
            if (commit.getRevisionId() == revisionId) {
                return commit;
            }
        }
        return null;
    }

    private List<CommitMongo> getCommits(long fromRevisionId, long toRevisionId) {
        FetchCommitsAction query = new FetchCommitsAction(mongoConnection, fromRevisionId,
                toRevisionId);
        return query.execute();
    }

    private Node getNode(String path, long revisionId) throws Exception {
        GetNodesCommand command = new GetNodesCommand(mongoConnection,
                path, revisionId);
        return command.execute();
    }
}