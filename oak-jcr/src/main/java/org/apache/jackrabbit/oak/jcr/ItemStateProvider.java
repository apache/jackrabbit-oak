package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TransientNodeState;

public class ItemStateProvider {
    private final TransientNodeState root;

    ItemStateProvider(TransientNodeState root) {
        this.root = root;
    }

    TransientNodeState getNodeState(String path){
        TransientNodeState state = root;

        for (String name : PathUtils.elements(path)) {
            state = state.getChildNode(name);
            if (state == null) {
                return null;
            }
        }

        return state;
    }

    PropertyState getPropertyState(String path) {
        TransientNodeState parentState = getNodeState(PathUtils.getParentPath(path));

        if (parentState == null) {
            return null;
        }
        else {
            return parentState.getProperty(PathUtils.getName(path));
        }
    }
}
