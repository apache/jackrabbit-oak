/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.property.jmx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeTraverser;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(service = {})
public class PropertyIndexStats extends AnnotatedStandardMBean implements PropertyIndexStatsMBean {

    @Reference
    private NodeStore store;

    private Registration reg;

    public PropertyIndexStats() {
        super(PropertyIndexStatsMBean.class);
    }

    @Activate
    private void activate(BundleContext context) {
        reg = registerMBean(new OsgiWhiteboard(context),
                PropertyIndexStatsMBean.class,
                this,
                PropertyIndexStatsMBean.TYPE,
                "Property Index statistics");
    }

    @Deactivate
    private void deactivate() {
        if (reg != null) {
            reg.unregister();
        }
    }

    @Override
    public TabularData getStatsForAllIndexes(String path, int maxValueCount, int maxDepth, int maxPathCount)
            throws OpenDataException {
        String indexRootPath = concat(path, "oak:index");
        NodeState idxRoot = NodeStateUtils.getNode(store.getRoot(), indexRootPath);
        TabularType tt = new TabularType(PropertyIndexStats.class.getName(),
                "Property Index Stats", getType(), new String[]{"path"});
        TabularDataSupport tds = new TabularDataSupport(tt);
        for(ChildNodeEntry cne : idxRoot.getChildNodeEntries()){
            if ("property".equals(cne.getNodeState().getString("type"))) {
                CompositeData stats = getStatsForIndex(concat(indexRootPath, cne.getName()), cne.getNodeState(),
                        maxValueCount, maxDepth, maxPathCount);
                tds.put(stats);
            }
        }
        return tds;
    }


    @Override
    public CompositeData getStatsForSpecificIndex(String path, int maxValueCount, int maxDepth, int maxPathCount) throws
            OpenDataException {
        NodeState idx = NodeStateUtils.getNode(store.getRoot(), path);
        return getStatsForIndex(path, idx, maxValueCount, maxDepth, maxPathCount);
    }

    private CompositeData getStatsForIndex(String path, NodeState idx, int maxValueCount, int maxDepth, int maxPathCount)
            throws OpenDataException {
        Map<String, Object> result = new HashMap<String, Object>();

        //Add placeholder
        result.put("path", path);
        result.put("values", new String[0]);
        result.put("paths", new String[0]);
        result.put("valueCount", -1L);
        result.put("pathCount", -1);
        result.put("maxPathCount", maxPathCount);
        result.put("maxDepth", maxDepth);
        result.put("maxValueCount", maxValueCount);

        String status = "No index found at path " + path;
        NodeState data = idx.getChildNode(INDEX_CONTENT_NODE_NAME);
        if (data.exists()) {
            if (idx.getBoolean(UNIQUE_PROPERTY_NAME)) {
                status = "stats not supported for unique indexes";
            } else {
                long childNodeCount = data.getChildNodeCount(maxValueCount);
                if (childNodeCount == Long.MAX_VALUE || childNodeCount > maxValueCount) {
                    status = String.format("stats cannot be determined as number of values exceed the max limit of " +
                            "[%d]. Estimated value count [%d]", maxValueCount, childNodeCount);
                } else {
                    String[] values = Iterables.toArray(
                            Iterables.limit(data.getChildNodeNames(), maxValueCount),
                            String.class
                    );

                    String[] paths = determineIndexedPaths(data.getChildNodeEntries(), maxDepth, maxPathCount);
                    result.put("values", values);
                    result.put("paths", paths);
                    result.put("pathCount", paths.length);
                    status = "Result determined and above path list can be safely used based on current indexed data";
                }
                result.put("valueCount", childNodeCount);
            }
        }

        result.put("status", status);
        return new CompositeDataSupport(getType(), result);
    }

    private String[] determineIndexedPaths(Iterable<? extends ChildNodeEntry> values,
                                           final int maxDepth, int maxPathCount) {
        Set<String> paths = Sets.newHashSet();
        Set<String> intermediatePaths = Sets.newHashSet();
        int maxPathLimitBreachedAtLevel = -1;
        topLevel:
        for (ChildNodeEntry cne : values) {
            Tree t = TreeFactory.createReadOnlyTree(cne.getNodeState());
            TreeTraverser<Tree> traverser = new TreeTraverser<Tree>() {
                @Override
                public Iterable<Tree> children(@Nonnull  Tree root) {
                    //Break at maxLevel
                    if (PathUtils.getDepth(root.getPath()) >= maxDepth) {
                        return Collections.emptyList();
                    }
                    return root.getChildren();
                }
            };
            for (Tree node : traverser.breadthFirstTraversal(t)) {
                PropertyState matchState = node.getProperty("match");
                boolean match = matchState == null ? false : matchState.getValue(Type.BOOLEAN);
                int depth = PathUtils.getDepth(node.getPath());

                //Intermediate nodes which are not leaf are not to be included
                if (depth < maxDepth && !match) {
                    intermediatePaths.add(node.getPath());
                    continue;
                }

                if (paths.size() < maxPathCount) {
                    paths.add(node.getPath());
                } else {
                    maxPathLimitBreachedAtLevel = depth;
                    break topLevel;
                }
            }
        }

        if (maxPathLimitBreachedAtLevel < 0) {
            return Iterables.toArray(paths, String.class);
        }

        //If max limit for path is reached then we can safely
        //say about includedPaths upto depth = level at which limit reached - 1
        //As for that level we know *all* the path roots
        Set<String> result = Sets.newHashSet();
        int safeDepth = maxPathLimitBreachedAtLevel - 1;
        if (safeDepth > 0) {
            for (String path : intermediatePaths) {
                int pathDepth = PathUtils.getDepth(path);
                if (pathDepth == safeDepth) {
                    result.add(path);
                }
            }
        }
        return Iterables.toArray(result, String.class);
    }

    @SuppressWarnings("unchecked")
    private static CompositeType getType() throws OpenDataException {
        return new CompositeType("PropertyIndexStats", "Property index related stats",
                new String[]{"path", "values", "paths", "valueCount", "status", "pathCount", "maxPathCount",
                        "maxDepth", "maxValueCount"},
                new String[]{"path", "values", "paths", "valueCount", "status", "pathCount", "maxPathCount",
                        "maxDepth", "maxValueCount"},
                new OpenType[]{
                        SimpleType.STRING,
                        new ArrayType(SimpleType.STRING, false),
                        new ArrayType(SimpleType.STRING, false),
                        SimpleType.LONG,
                        SimpleType.STRING,
                        SimpleType.INTEGER,
                        SimpleType.INTEGER,
                        SimpleType.INTEGER,
                        SimpleType.INTEGER,
                });

    }
}
