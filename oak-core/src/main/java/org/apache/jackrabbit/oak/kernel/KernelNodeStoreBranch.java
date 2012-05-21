package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * {@code NodeStoreBranch} based on {@link MicroKernel} branching and merging.
 * This implementation keeps changes in memory up to a certain limit and writes
 * them back when the to the Microkernel branch when the limit is exceeded.
 */
class KernelNodeStoreBranch implements NodeStoreBranch {

    /** The underlying store to which this branch belongs */
    private final KernelNodeStore store;

    /** Base state of this branch */
    private final NodeState base;


    /** Revision of this branch in the Microkernel */
    private String branchRevision;

    /** Current root state of this branch */
    private NodeState currentRoot;

    /** Last state which was committed to this branch */
    private NodeState committed;

    KernelNodeStoreBranch(KernelNodeStore store) {
        this.store = store;

        MicroKernel kernel = getKernel();
        this.branchRevision = kernel.branch(null);
        this.currentRoot = new KernelNodeState(kernel, getValueFactory(), "/", branchRevision);
        this.base = currentRoot;
        this.committed = currentRoot;
    }

    @Override
    public NodeState getRoot() {
        return currentRoot;
    }

    @Override
    public NodeState getBase() {
        return base;
    }

    @Override
    public void setRoot(NodeState newRoot) {
        currentRoot = newRoot;
        save(buildJsop());
    }

    @Override
    public boolean move(String source, String target) {
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        save(buildJsop() + ">\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public boolean copy(String source, String target) {
        if (getNode(source) == null) {
            // source does not exist
            return false;
        }
        NodeState destParent = getNode(getParentPath(target));
        if (destParent == null) {
            // parent of destination does not exist
            return false;
        }
        if (destParent.getChildNode(getName(target)) != null) {
            // destination exists already
            return false;
        }

        save(buildJsop() + "*\"" + source + "\":\"" + target + '"');
        return true;
    }

    @Override
    public KernelNodeState merge() throws CommitFailedException {
        save(buildJsop());
        // TODO rebase, call commitHook (OAK-100)
        MicroKernel kernel = getKernel();
        String mergedRevision;
        try {
            mergedRevision = kernel.merge(branchRevision, null);
            branchRevision = null;
            currentRoot = null;
            committed = null;
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
        return new KernelNodeState(kernel, getValueFactory(), "/", mergedRevision);
    }

    //------------------------------------------------------------< private >---

    private MicroKernel getKernel() {
        return store.getKernel();
    }

    private CoreValueFactory getValueFactory() {
        return store.getValueFactory();
    }

    private NodeState getNode(String path) {
        NodeState node = getRoot();
        for (String name : elements(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                break;
            }
        }

        return node;
    }

    private void save(String jsop) {
        MicroKernel kernel = getKernel();
        branchRevision = kernel.commit("/", jsop, branchRevision, null);
        currentRoot = new KernelNodeState(kernel, getValueFactory(), "/", branchRevision);
        committed = currentRoot;
    }

    private String buildJsop() {
        StringBuilder jsop = new StringBuilder();
        diffToJsop(committed, currentRoot, "", jsop);
        return jsop.toString();
    }

    private void diffToJsop(NodeState before, NodeState after, final String path,
            final StringBuilder jsop) {

        store.compare(before, after, new NodeStateDiff() {
            @Override
            public void propertyAdded(PropertyState after) {
                jsop.append('^').append(buildPath(after.getName()))
                        .append(':').append(toJson(after));
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                jsop.append('^').append(buildPath(after.getName()))
                        .append(':').append(toJson(after));
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                jsop.append('^').append(buildPath(before.getName())).append(":null");
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                jsop.append('+').append(buildPath(name)).append(':');
                toJson(after);
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                jsop.append('-').append(buildPath(name));
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                diffToJsop(before, after, PathUtils.concat(path, name), jsop);
            }

            private String buildPath(String name) {
                return '"' + PathUtils.concat(path, name) + '"';
            }

            private String toJson(PropertyState propertyState) {
                return propertyState.isArray()
                    ? CoreValueMapper.toJsonArray(propertyState.getValues())
                    : CoreValueMapper.toJsonValue(propertyState.getValue());
            }

            private void toJson(NodeState nodeState) {
                jsop.append('{');
                String comma = "";
                for (PropertyState property : nodeState.getProperties()) {
                    String value = property.isArray()
                            ? CoreValueMapper.toJsonArray(property.getValues())
                            : CoreValueMapper.toJsonValue(property.getValue());

                    jsop.append(comma);
                    comma = ",";
                    jsop.append('"').append(property.getName()).append("\":").append(value);
                }

                for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
                    jsop.append(comma);
                    comma = ",";
                    jsop.append('"').append(child.getName()).append("\":");
                    toJson(child.getNodeState());
                }
                jsop.append('}');
            }
        });
    }
}
