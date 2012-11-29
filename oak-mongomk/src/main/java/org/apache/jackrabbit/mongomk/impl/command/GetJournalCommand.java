package org.apache.jackrabbit.mongomk.impl.command;

import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
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
        if (toCommit != null && toCommit.getBranchId() != null) {
            throw new MicroKernelException("Branch revisions are not supported: " + toRevisionId);
        }

        MongoCommit fromCommit;
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
            }
            commitBuff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage())
            .key("changes").value(diff).endObject();
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
}