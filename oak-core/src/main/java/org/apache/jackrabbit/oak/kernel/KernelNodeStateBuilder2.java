package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore2.NodeStateBuilderContext;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;

import java.util.List;

public class KernelNodeStateBuilder2 implements NodeStateBuilder {
    private final NodeStateBuilderContext context;

    private String path;

    private KernelNodeStateBuilder2(NodeStateBuilderContext context, String path) {
        this.context = context;
        this.path = path;
    }

    public static NodeStateBuilder create(NodeStateBuilderContext context) {
        return new KernelNodeStateBuilder2(context, "");
    }


    @Override
    public NodeState getNodeState() {
        return context.getNodeState(path);
    }

    @Override
    public NodeStateBuilder getChildBuilder(String name) {
        return hasChild(name)
            ? new KernelNodeStateBuilder2(context, PathUtils.concat(path, name))
            : null;
    }

    @Override
    public NodeStateBuilder addNode(String name, NodeState nodeState) {
        if (hasChild(name)) {
            return null;
        }
        else {
            String targetPath = PathUtils.concat(path, name);
            context.addNode(nodeState, targetPath);
            return new KernelNodeStateBuilder2(context, targetPath);
        }
    }

    @Override
    public NodeStateBuilder addNode(String name) {
        if (hasChild(name)) {
            return null;
        }
        else {
            String targetPath = PathUtils.concat(path, name);
            context.addNode(targetPath);
            return new KernelNodeStateBuilder2(context, targetPath);
        }
    }

    @Override
    public boolean removeNode(String name) {
        if (hasChild(name)) {
            context.removeNode(PathUtils.concat(path, name));
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void setProperty(String name, CoreValue value) {
        PropertyState property = new PropertyStateImpl(name, value);
        if (hasProperty(name)) {
            context.setProperty(property, path);
        }
        else {
            context.addProperty(property, path);
        }
    }

    @Override
    public void setProperty(String name, List<CoreValue> values) {
        PropertyState property = new PropertyStateImpl(name, values);
        if (hasProperty(name)) {
            context.setProperty(property, path);
        }
        else {
            context.addProperty(property, path);
        }
    }

    @Override
    public void removeProperty(String name) {
        if (hasProperty(name)) {
            context.removeProperty(PathUtils.concat(path, name));
        }
    }

    @Override
    public boolean moveTo(NodeStateBuilder destParent, String destName) {
        if (!(destParent instanceof KernelNodeStateBuilder2)) {
            throw new IllegalArgumentException("Alien builder for destParent");
        }

        if (destParent.getChildBuilder(destName) != null) {
            return false;
        }

        KernelNodeStateBuilder2 destParentBuilder = (KernelNodeStateBuilder2) destParent;
        String destPath = PathUtils.concat(destParentBuilder.path, destName);

        context.moveNode(path, destPath);
        path = destPath;
        return true;
    }

    @Override
    public boolean copyTo(NodeStateBuilder destParent, String destName) {
        if (!(destParent instanceof KernelNodeStateBuilder2)) {
            throw new IllegalArgumentException("Alien builder for destParent");
        }

        if (destParent.getChildBuilder(destName) != null) {
            return false;
        }

        KernelNodeStateBuilder2 destParentBuilder = (KernelNodeStateBuilder2) destParent;
        String destPath = PathUtils.concat(destParentBuilder.path, destName);

        context.copyNode(path, destPath);
        return true;
    }

    //------------------------------------------------------------< internal >---

    NodeStateBuilderContext getContext() {
        return context;
    }

    //------------------------------------------------------------< private >---

    private boolean hasChild(String name) {
        return getNodeState().getChildNode(name) != null;
    }

    private boolean hasProperty(String name) {
        return getNodeState().getProperty(name) != null;
    }

}
