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

import javax.annotation.CheckForNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;

/**
 * A ChangeSet is a collection of items that have been changed as part of a
 * commit. A ChangeSet is immutable and built by a ChangeSetBuilder.
 * <p>
 * Those items are parent paths, parent node names, parent node types and
 * (child) properties. 'Changed' refers to any of add, remove, change (where
 * applicable).
 * <p>
 * A ChangeSet is piggybacked on a CommitInfo in the CommitContext and can be
 * used by (downstream) Observers for their convenience.
 * <p>
 * To limit memory usage, the ChangeSet has a limit on the number of items,
 * each, that it collects. If one of those items reach the limit this is called
 * an 'overflow' and the corresponding item type is marked as having
 * 'overflown'. Downstream Observers should thus check if a particular item has
 * overflown or not - this is indicated with null as the return value of the
 * corresponding getters (while empty means: not overflown but nothing changed
 * of that type).
 * <p>
 * Also, the ChangeSet carries a 'maxPathDepth' which is the depth of the path
 * up until which paths have been collected. Thus any path that is longer than
 * this 'maxPathDepth' will be cut off and only reported up to that max depth.
 * Downstream Observers should thus inspect the 'maxPathDepth' and compare
 * actual path depths with it in order to find out if any child paths have been
 * cut off.
 * <p>
 * Naming: note that path, node name and node types all refer to the *parent* of
 * a change. While properties naturally are leafs.
 */
public final class ChangeSet {

    public static final String COMMIT_CONTEXT_OBSERVATION_CHANGESET = "oak.observation.changeSet";

    private final int maxPathDepth;
    private final Set<String> parentPaths;
    private final Set<String> parentNodeNames;
    private final Set<String> parentNodeTypes;
    private final Set<String> propertyNames;
    private final Set<String> allNodeTypes;
    private final boolean hitsMaxPathDepth;

    ChangeSet(int maxPathDepth, Set<String> parentPaths, Set<String> parentNodeNames, Set<String> parentNodeTypes,
            Set<String> propertyNames, Set<String> allNodeTypes) {
        this.maxPathDepth = maxPathDepth;
        this.parentPaths = parentPaths == null ? null : ImmutableSet.copyOf(parentPaths);
        this.parentNodeNames = parentNodeNames == null ? null : ImmutableSet.copyOf(parentNodeNames);
        this.parentNodeTypes = parentNodeTypes == null ? null : ImmutableSet.copyOf(parentNodeTypes);
        this.propertyNames = propertyNames == null ? null : ImmutableSet.copyOf(propertyNames);
        this.allNodeTypes = allNodeTypes == null ? null : ImmutableSet.copyOf(allNodeTypes);
        
        boolean hitsMaxPathDepth = false;
        if (parentPaths != null) {
            for (String aPath : parentPaths) {
                if (PathUtils.getDepth(aPath) >= maxPathDepth) {
                    hitsMaxPathDepth = true;
                    break;
                }
            }
        }
        this.hitsMaxPathDepth = hitsMaxPathDepth;
    }

    @Override
    public String toString() {
        return "ChangeSet{paths[maxDepth:" + maxPathDepth + "]=" + parentPaths + ", propertyNames=" + propertyNames
                + ", parentNodeNames=" + parentNodeNames + ", parentNodeTypes=" + parentNodeTypes 
                + ", allNodeTypes=" + allNodeTypes + ", any overflow: " + anyOverflow()
                + ", hits max path depth: " + hitsMaxPathDepth + "}";
    }

    public boolean doesHitMaxPathDepth() {
        return hitsMaxPathDepth;
    }

    @CheckForNull
    public Set<String> getParentPaths() {
        return parentPaths;
    }

    @CheckForNull
    public Set<String> getParentNodeNames() {
        return parentNodeNames;
    }

    @CheckForNull
    public Set<String> getParentNodeTypes() {
        return parentNodeTypes;
    }

    @CheckForNull
    public Set<String> getPropertyNames() {
        return propertyNames;
    }

    public int getMaxPrefilterPathDepth() {
        return maxPathDepth;
    }

    @CheckForNull
    public Set<String> getAllNodeTypes() {
        return allNodeTypes;
    }
    
    public boolean anyOverflow() {
        return getAllNodeTypes() == null || 
                getParentNodeNames() == null ||
                getParentNodeTypes() == null ||
                getParentPaths() == null ||
                getPropertyNames() == null;
    }

    //~---------------------------------------------------< equals/hashcode >

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChangeSet changeSet = (ChangeSet) o;

        if (maxPathDepth != changeSet.maxPathDepth) return false;
        if (parentPaths != null ? !parentPaths.equals(changeSet.parentPaths) : changeSet.parentPaths != null)
            return false;
        if (parentNodeNames != null ? !parentNodeNames.equals(changeSet.parentNodeNames) : changeSet.parentNodeNames != null)
            return false;
        if (parentNodeTypes != null ? !parentNodeTypes.equals(changeSet.parentNodeTypes) : changeSet.parentNodeTypes != null)
            return false;
        if (propertyNames != null ? !propertyNames.equals(changeSet.propertyNames) : changeSet.propertyNames != null)
            return false;
        return allNodeTypes != null ? allNodeTypes.equals(changeSet.allNodeTypes) : changeSet.allNodeTypes == null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    //~----------------------------------------------------< json support >

    public String asString(){
        JsopWriter json = new JsopBuilder();
        json.object();
        json.key("maxPathDepth").value(maxPathDepth);
        addToJson(json, "parentPaths", parentPaths);
        addToJson(json, "parentNodeNames", parentNodeNames);
        addToJson(json, "parentNodeTypes", parentNodeTypes);
        addToJson(json, "propertyNames", propertyNames);
        addToJson(json, "allNodeTypes", allNodeTypes);
        json.endObject();
        return json.toString();
    }

    public static ChangeSet fromString(String json) {
        JsopReader reader = new JsopTokenizer(json);
        int maxPathDepth = 0;
        Set<String> parentPaths = null;
        Set<String> parentNodeNames = null;
        Set<String> parentNodeTypes = null;
        Set<String> propertyNames = null;
        Set<String> allNodeTypes = null;

        reader.read('{');
        if (!reader.matches('}')) {
            do {
                String name = reader.readString();
                reader.read(':');
                if ("maxPathDepth".equals(name)){
                    maxPathDepth = Integer.parseInt(reader.read(JsopReader.NUMBER));
                } else {
                    Set<String> data = readArrayAsSet(reader);
                    if ("parentPaths".equals(name)){
                        parentPaths = data;
                    } else if ("parentNodeNames".equals(name)){
                        parentNodeNames = data;
                    } else if ("parentNodeTypes".equals(name)){
                        parentNodeTypes = data;
                    } else if ("propertyNames".equals(name)){
                        propertyNames = data;
                    } else if ("allNodeTypes".equals(name)){
                        allNodeTypes = data;
                    }
                }
            } while (reader.matches(','));
            reader.read('}');
        }
        reader.read(JsopReader.END);
        return new ChangeSet(maxPathDepth, parentPaths, parentNodeNames, parentNodeTypes, propertyNames, allNodeTypes);
    }

    private static Set<String> readArrayAsSet(JsopReader reader) {
        Set<String> values = Sets.newHashSet();
        reader.read('[');
        for (boolean first = true; !reader.matches(']'); first = false) {
            if (!first) {
                reader.read(',');
            }
            values.add(reader.readString());
        }
        return values;
    }

    private static void addToJson(JsopWriter json, String name, Set<String> values){
        if (values == null){
            return;
        }
        json.key(name).array();
        for (String v : values){
            json.value(v);
        }
        json.endArray();
    }
}