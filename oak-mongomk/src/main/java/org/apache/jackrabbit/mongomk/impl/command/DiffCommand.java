package org.apache.jackrabbit.mongomk.impl.command;

import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.model.CommitMongo;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.util.MongoUtil;

/**
 * A {@code Command} for {@code MongoMicroKernel#diff(String, String, String, int)}
 */
public class DiffCommand extends BaseCommand<String> {

    private final String fromRevision;
    private final String toRevision;
    private final int depth;

    private String path;

    /**
     * Constructs a {@code DiffCommandCommandMongo}
     *
     * @param mongoConnection Mongo connection
     * @param fromRevision From revision id.
     * @param toRevision To revision id.
     * @param path Path.
     * @param depth Depth.
     */
    public DiffCommand(MongoConnection mongoConnection, String fromRevision,
            String toRevision, String path, int depth) {
        super(mongoConnection);
        this.fromRevision = fromRevision;
        this.toRevision = toRevision;
        this.path = path;
        this.depth = depth;
    }

    @Override
    public String execute() throws Exception {
        path = MongoUtil.adjustPath(path);
        checkDepth();

        long fromRevisionId, toRevisionId;
        if (fromRevision == null || toRevision == null) {
            FetchHeadRevisionIdAction query = new FetchHeadRevisionIdAction(mongoConnection);
            query.includeBranchCommits(true);
            long head = query.execute();
            fromRevisionId = fromRevision == null? head : MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = toRevision == null ? head : MongoUtil.toMongoRepresentation(toRevision);;
        } else {
            fromRevisionId = MongoUtil.toMongoRepresentation(fromRevision);
            toRevisionId = MongoUtil.toMongoRepresentation(toRevision);;
        }

        if (fromRevisionId == toRevisionId) {
            return "";
        }

        if ("/".equals(path)) {
            CommitMongo toCommit = new FetchCommitAction(mongoConnection, toRevisionId).execute();
            if (toCommit.getBaseRevId() == fromRevisionId) {
                // Specified range spans a single commit:
                // use diff stored in commit instead of building it dynamically
                return toCommit.getDiff();
            }
        }

        NodeState beforeState = MongoUtil.wrap(getNode(path, fromRevisionId));
        NodeState afterState = MongoUtil.wrap(getNode(path, toRevisionId));

        return new DiffBuilder(beforeState, afterState, path, depth,
                new MongoNodeStore(), path).build();
    }

    private void checkDepth() {
        if (depth < -1) {
            throw new IllegalArgumentException("depth");
        }
    }

    private Node getNode(String path, long revisionId) throws Exception {
        GetNodesCommand command = new GetNodesCommand(mongoConnection,
                path, revisionId);
        return command.execute();
    }
}