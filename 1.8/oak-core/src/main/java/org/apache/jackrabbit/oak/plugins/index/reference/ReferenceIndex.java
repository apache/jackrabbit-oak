/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.reference;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Double.POSITIVE_INFINITY;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.WEAKREFERENCE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.REF_NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.WEAK_REF_NAME;
import static org.apache.jackrabbit.oak.plugins.index.Cursors.newPathCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Provides a QueryIndex that does lookups for node references based on a custom
 * index saved on hidden property names
 * 
 */
class ReferenceIndex implements QueryIndex {

    private static final double COST = 1;

    private final MountInfoProvider mountInfoProvider;

    ReferenceIndex() {
        this(Mounts.defaultMountInfoProvider());
    }

    ReferenceIndex(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public double getMinimumCost() {
        return COST;
    }

    @Override
    public String getIndexName() {
        return NAME;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // TODO don't call getCost for such queries
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return POSITIVE_INFINITY;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return Double.POSITIVE_INFINITY;
        }
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (isEqualityRestrictionOnType(pr, REFERENCE) ||
                    isEqualityRestrictionOnType(pr, WEAKREFERENCE)) {
                return COST;
            }
        }
        // not an appropriate index
        return POSITIVE_INFINITY;
    }
    
    private static boolean isEqualityRestrictionOnType(PropertyRestriction pr, int propertyType) {
        if (pr.propertyType != propertyType) {
            return false;
        }
        return pr.first != null && pr.first == pr.last;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (isEqualityRestrictionOnType(pr, REFERENCE)) {
                String uuid = pr.first.getValue(STRING);
                String name = pr.propertyName;
                return lookup(root, uuid, name, REF_NAME, filter);
            }
            if (isEqualityRestrictionOnType(pr, WEAKREFERENCE)) {
                String uuid = pr.first.getValue(STRING);
                String name = pr.propertyName;
                return lookup(root, uuid, name, WEAK_REF_NAME, filter);
            }
        }
        return newPathCursor(new ArrayList<String>(), filter.getQueryLimits());
    }

    private Cursor lookup(NodeState root, String uuid,
            final String name, String index, Filter filter) {
        NodeState indexRoot = root.getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode(NAME);
        if (!indexRoot.exists()) {
            return newPathCursor(new ArrayList<String>(), filter.getQueryLimits());
        }
        List<Iterable<String>> iterables = Lists.newArrayList();
        for (IndexStoreStrategy s : getStrategies(indexRoot, mountInfoProvider, index)) {
            iterables.add(s.query(filter, index + "("
                    + uuid + ")", indexRoot, ImmutableSet.of(uuid)));
        }
        Iterable<String> paths = Iterables.concat(iterables);

        if (!"*".equals(name)) {
            paths = filter(paths, new Predicate<String>() {
                @Override
                public boolean apply(String path) {
                    return name.equals(getName(path));
                }
            });
        }
        paths = transform(paths, new Function<String, String>() {
            @Override
            public String apply(String path) {
                return getParentPath(path);
            }
        });
        return newPathCursor(paths, filter.getQueryLimits());
    }

    private static Set<IndexStoreStrategy> getStrategies(NodeState definition,
            MountInfoProvider mountInfoProvider, String index) {
        return Multiplexers.getStrategies(false, mountInfoProvider, definition,
                index);
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        StringBuilder buff = new StringBuilder("reference");
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (pr.propertyType == REFERENCE) {
                buff.append(" PROPERTY([");
                buff.append(pr.propertyName);
                buff.append("], 'Reference') = ");
                buff.append(pr.first.getValue(STRING));
                return buff.toString();
            }
            if (pr.propertyType == WEAKREFERENCE) {
                buff.append(" PROPERTY([");
                buff.append(pr.propertyName);
                buff.append("], 'WeakReference') = ");
                buff.append(pr.first.getValue(STRING));
                return buff.toString();
            }
        }
        return buff.toString();
    }

}
