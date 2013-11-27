package org.apache.jackrabbit.oak.plugins.observation.filter;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
* michid document
*/
public class ConstantFilter implements Filter {
    public static final ConstantFilter INCLUDE_ALL = new ConstantFilter(true);
    public static final ConstantFilter EXCLUDE_ALL = new ConstantFilter(false);

    private final boolean include;

    public ConstantFilter(boolean include) {
        this.include = include;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return include;
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return include;
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return include;
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return include;
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return include;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return include;
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return include;
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        return include ? this : null;
    }
}
