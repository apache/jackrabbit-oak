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
package org.apache.jackrabbit.oak.spi.observation;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Builder of a ChangeSet - only used by ChangeCollectorProvider (and tests..)
 */
public class ChangeSetBuilder {

    private final int maxItems;
    private int maxPathDepth;
    private final Set<String> parentPaths = Sets.newHashSet();
    private final Set<String> parentNodeNames = Sets.newHashSet();
    private final Set<String> parentNodeTypes = Sets.newHashSet();
    private final Set<String> propertyNames = Sets.newHashSet();
    private final Set<String> allNodeTypes = Sets.newHashSet();

    private boolean parentPathOverflow;
    private boolean parentNodeNameOverflow;
    private boolean parentNodeTypeOverflow;
    private boolean propertyNameOverflow;
    private boolean allNodeTypeOverflow;

    public ChangeSetBuilder(int maxItems, int maxPathDepth) {
        this.maxItems = maxItems;
        this.maxPathDepth = maxPathDepth;
    }

    @Override
    public String toString() {
        return "ChangeSetBuilder{parentPaths[maxDepth:" + maxPathDepth + "]=" + parentPaths + ", propertyNames="
                + propertyNames + ", parentNodeNames=" + parentNodeNames + ", parentNodeTypes=" + parentNodeTypes
                + ", allNodeTypes=" + allNodeTypes + "}";
    }

    public boolean isParentPathOverflown() {
        return parentPathOverflow;
    }

    public ChangeSetBuilder addParentPath(String path){
        path = getPathWithMaxDepth(path, maxPathDepth);
        parentPathOverflow = addAndCheckOverflow(parentPaths, path, maxItems, parentPathOverflow);
        return this;
    }

    public boolean isParentNodeNameOverflown() {
        return parentNodeNameOverflow;
    }

    public ChangeSetBuilder addParentNodeName(String parentNodeName) {
        parentNodeNameOverflow = addAndCheckOverflow(parentNodeNames, parentNodeName, maxItems, parentNodeNameOverflow);
        return this;
    }

    public boolean isParentNodeTypeOverflown() {
        return parentNodeTypeOverflow;
    }

    public ChangeSetBuilder addParentNodeTypes(Iterable<String> nodeTypes){
        for (String nodeType : nodeTypes){
            addParentNodeType(nodeType);
        }
        return this;
    }

    public ChangeSetBuilder addParentNodeType(String parentNodeType) {
        parentNodeTypeOverflow = addAndCheckOverflow(parentNodeTypes, parentNodeType, maxItems, parentNodeTypeOverflow);
        return this;
    }

    public boolean isPropertyNameOverflown() {
        return propertyNameOverflow;
    }

    public ChangeSetBuilder addPropertyName(String propertyName) {
        propertyNameOverflow = addAndCheckOverflow(propertyNames, propertyName, maxItems, propertyNameOverflow);
        return this;
    }
    public boolean isAllNodeTypeOverflown() {
        return allNodeTypeOverflow;
    }

    public ChangeSetBuilder addNodeTypes(Iterable<String> nodeTypes){
        for (String nodeType : nodeTypes){
            addNodeType(nodeType);
        }
        return this;
    }

    public ChangeSetBuilder addNodeType(String nodeType) {
        allNodeTypeOverflow = addAndCheckOverflow(allNodeTypes, nodeType, maxItems, allNodeTypeOverflow);
        return this;
    }

    public int getMaxPrefilterPathDepth() {
        return maxPathDepth;
    }

    public ChangeSetBuilder add(@Nullable ChangeSet cs){
        if (cs == null){
            parentPathOverflow = true;
            parentNodeNameOverflow = true;
            parentNodeTypeOverflow = true;
            propertyNameOverflow = true;
            allNodeTypeOverflow = true;
            return this;
        }

        if (cs.getParentPaths() == null){
            parentPathOverflow = true;
        } else {
            addPathFromChangeSet(cs);
        }

        if (cs.getParentNodeNames() == null){
            parentNodeNameOverflow = true;
        } else {
            for (String parentNodeName : cs.getParentNodeNames()){
                addParentNodeName(parentNodeName);
            }
        }

        if (cs.getParentNodeTypes() == null){
            parentNodeTypeOverflow = true;
        } else {
            for (String parentNodeType : cs.getParentNodeTypes()){
                addParentNodeType(parentNodeType);
            }
        }

        if (cs.getPropertyNames() == null){
            propertyNameOverflow = true;
        } else {
            for (String propertyName : cs.getPropertyNames()){
                addPropertyName(propertyName);
            }
        }

        if (cs.getAllNodeTypes() == null){
            allNodeTypeOverflow = true;
        } else {
            for (String nodeType : cs.getAllNodeTypes()){
                addNodeType(nodeType);
            }
        }

        return this;
    }

    public ChangeSet build() {
        return new ChangeSet(maxPathDepth, parentPathOverflow ? null : parentPaths,
                parentNodeNameOverflow ? null : parentNodeNames,
                parentNodeTypeOverflow ? null : parentNodeTypes,
                propertyNameOverflow ? null : propertyNames,
                allNodeTypeOverflow ? null : allNodeTypes);
    }

    private void addPathFromChangeSet(ChangeSet cs) {
        int maxDepthInChangeSet = cs.getMaxPrefilterPathDepth();

        //If maxDepth of ChangeSet being added is less than current
        //then truncate path in current set to that depth and change
        //maxPathDepth to one from ChangeSet
        if (maxDepthInChangeSet < maxPathDepth){
            Set<String> existingPathSet = Sets.newHashSet(parentPaths);
            parentPaths.clear();
            for (String existingPath : existingPathSet){
               parentPaths.add(getPathWithMaxDepth(existingPath, maxDepthInChangeSet));
            }
            maxPathDepth = maxDepthInChangeSet;
        }

        for (String pathFromChangeSet : cs.getParentPaths()){
            addParentPath(getPathWithMaxDepth(pathFromChangeSet, maxPathDepth));
        }
    }

    private static String getPathWithMaxDepth(String path, int maxDepth){
        int depth = PathUtils.getDepth(path);
        if (depth <= maxDepth){
            return path;
        }
        return PathUtils.getAncestorPath(path, depth - maxDepth);
    }

    /**
     * Add data to dataSet if dataSet size is less than maxSize.
     *
     * @return true if dataSet size is at maxSize i.e. overflow condition reached
     */
    private static boolean addAndCheckOverflow(Set<String> dataSet, String data, int maxSize, boolean overflow) {
        if (overflow) {
            return true;
        } else {
            dataSet.add(data);
            if (dataSet.size() > maxSize){
                dataSet.clear();
                return true;
            }
            return false;
        }
    }

}
