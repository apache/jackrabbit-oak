package org.apache.jackrabbit.mongomk.impl.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mongomk.action.FetchCommitsAction;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeStore;
import org.apache.jackrabbit.mongomk.model.CommitMongo;
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
     * @param mongoConnection Mongo connection.
     * @param since Timestamp (ms) of earliest revision to be returned
     * @param maxEntries maximum #entries to be returned; if < 0, no limit will be applied.
     * @param path optional path filter; if {@code null} or {@code ""} the
     * default ({@code "/"}) will be assumed, i.e. no filter will be applied
     */
    public GetRevisionHistoryCommand(MongoConnection mongoConnection,
            long since, int maxEntries, String path) {
        super(mongoConnection);
        this.since = since;
        this.maxEntries = maxEntries;
        this.path = path;
    }

    @Override
    public String execute() {
        path = MongoUtil.adjustPath(path);
        maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;

        FetchCommitsAction action = new FetchCommitsAction(mongoConnection);
        action.setMaxEntries(maxEntries);
        action.includeBranchCommits(false);

        List<CommitMongo> commits = action.execute();
        List<CommitMongo> history = new ArrayList<CommitMongo>();
        for (int i = commits.size() - 1; i >= 0; i--) {
            CommitMongo commit = commits.get(i);
            if (commit.getTimestamp() >= since) {
                if (MongoUtil.isFiltered(path)) {
                    try {
                        String diff = new DiffBuilder(
                                MongoUtil.wrap(getNode("/", commit.getBaseRevId())),
                                MongoUtil.wrap(getNode("/", commit.getRevisionId())),
                                "/", -1, new MongoNodeStore(), path).build();
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
        for (CommitMongo commit : history) {
            buff.object()
            .key("id").value(MongoUtil.fromMongoRepresentation(commit.getRevisionId()))
            .key("ts").value(commit.getTimestamp())
            .key("msg").value(commit.getMessage())
            .endObject();
        }
        return buff.endArray().toString();
    }

    private Node getNode(String path, long revisionId) throws Exception {
        GetNodesCommand command = new GetNodesCommand(mongoConnection,
                path, revisionId);
        return command.execute();
    }
}