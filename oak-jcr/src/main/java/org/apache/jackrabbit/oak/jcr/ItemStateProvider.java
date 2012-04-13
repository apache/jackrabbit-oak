package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.TransientNodeState;

class ItemStateProvider {
    private final TransientNodeState root;

    ItemStateProvider(TransientNodeState root) {
        this.root = root;
    }

    TransientNodeState getTransientNodeState(String path){
        TransientNodeState state = root;

        for (String name : Paths.elements(path)) {
            state = state.getChildNode(name);
            if (state == null) {
                return null;
            }
        }

        return state;
    }

    PropertyState getPropertyState(String path) {
        TransientNodeState parentState = getTransientNodeState(Paths.getParentPath(path));

        if (parentState == null) {
            return null;
        }
        else {
            return parentState.getProperty(Paths.getName(path));
        }
    }
}
