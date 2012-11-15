package org.apache.jackrabbit.mongomk.impl.command;

import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchCommitAction;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.tree.SimpleMongoNodeStore;
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
     * @param nodeStore Node store.
     * @param fromRevision From revision id.
     * @param toRevision To revision id.
     * @param path Path.
     * @param depth Depth.
     */
    public DiffCommand(MongoNodeStore nodeStore, String fromRevision,
            String toRevision, String path, int depth) {
        super(nodeStore);
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
            long head = new FetchHeadRevisionIdAction(nodeStore).execute();
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
            MongoCommit toCommit = new FetchCommitAction(nodeStore, toRevisionId).execute();
            if (toCommit.getBaseRevisionId() == fromRevisionId) {
                // Specified range spans a single commit:
                // use diff stored in commit instead of building it dynamically
                return toCommit.getDiff();
            }
        }

        NodeState beforeState = MongoUtil.wrap(getNode(path, fromRevisionId));
        NodeState afterState = MongoUtil.wrap(getNode(path, toRevisionId));

        return new DiffBuilder(beforeState, afterState, path, depth,
                new SimpleMongoNodeStore(), path).build();
    }

    private void checkDepth() {
        if (depth < -1) {
            throw new IllegalArgumentException("depth");
        }
    }

    private Node getNode(String path, long revisionId) throws Exception {
        GetNodesCommand command = new GetNodesCommand(nodeStore, path, revisionId);
        return command.execute();
    }
}