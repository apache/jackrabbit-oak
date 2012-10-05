package org.apache.jackrabbit.mongomk.impl.model.tree;

import org.apache.jackrabbit.mk.model.tree.AbstractPropertyState;

/**
 * FIXME - Consolidate with OAK.
 */
public class SimplePropertyState extends AbstractPropertyState {
    private final String name;
    private final String value;

    public SimplePropertyState(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getEncodedValue() {
        return value;
    }

}