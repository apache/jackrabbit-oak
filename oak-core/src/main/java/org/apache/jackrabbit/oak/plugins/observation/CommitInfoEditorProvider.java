package org.apache.jackrabbit.oak.plugins.observation;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Provider for a {@link Editor} that amends each commit with
 * a commit info record. That record is stored in a child node
 * named {@link #COMMIT_INFO} of the root node.
 */
public class CommitInfoEditorProvider implements EditorProvider {

    /**
     * Node name for the commit info record
     */
    public static final String COMMIT_INFO = ":commit-info";

    /**
     * Name of the property containing the id of the session that committed
     * this revision.
     * <p>
     * In a clustered environment this property might contain a synthesised
     * value if the respective revision is the result of a cluster sync.
     */
    public static final String SESSION_ID = "session-id";

    /**
     * Name of the property containing the id of the user that committed
     * this revision.
     * <p>
     * In a clustered environment this property might contain a synthesised
     * value if the respective revision is the result of a cluster sync.
     */
    public static final String USER_ID = "user-id";

    /**
     * Name of the property containing the time stamp when this revision was
     * committed.
     */
    public static final String TIME_STAMP = "time-stamp";

    private final String sessionId;
    private final String userId;

    public CommitInfoEditorProvider(@Nonnull String sessionId, String userID) {
        this.sessionId = checkNotNull(sessionId);
        this.userId = userID;
    }

    @Override
    public Editor getRootEditor(NodeState before, NodeState after, final NodeBuilder builder) {
        return new DefaultEditor() {
            @Override
            public void enter(NodeState before, NodeState after) {
                NodeBuilder commitInfo = builder.setChildNode(COMMIT_INFO);
                commitInfo.setProperty(USER_ID, String.valueOf(userId));
                commitInfo.setProperty(SESSION_ID, sessionId);
                commitInfo.setProperty(TIME_STAMP, System.currentTimeMillis());
            }
        };
    }
}
