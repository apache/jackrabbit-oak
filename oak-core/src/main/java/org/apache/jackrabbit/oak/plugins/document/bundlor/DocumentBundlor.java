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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class DocumentBundlor {
    /**
     * Hidden property to store the pattern as part of NodeState
     */
    public static final String META_PROP_PATTERN = ":pattern";

    public static final String PROP_PATTERN = "pattern";
    private final List<Include> includes;

    public static DocumentBundlor from(NodeState nodeState){
        Preconditions.checkArgument(nodeState.hasProperty(PROP_PATTERN), "NodeStated [%s] does not have required " +
                "property [%s]", nodeState, PROP_PATTERN);
       return DocumentBundlor.from(nodeState.getStrings(PROP_PATTERN));
    }

    public static DocumentBundlor from(Iterable<String> includeStrings){
        List<Include> includes = Lists.newArrayList();
        for (String i : includeStrings){
            includes.add(new Include(i));
        }
        return new DocumentBundlor(includes);
    }

    public static DocumentBundlor from(PropertyState prop){
        Preconditions.checkArgument(META_PROP_PATTERN.equals(prop.getName()));
        return from(prop.getValue(Type.STRINGS));
    }

    private DocumentBundlor(List<Include> includes) {
        this.includes = ImmutableList.copyOf(includes);
    }

    public boolean isBundled(String relativePath) {
        Matcher m = createMatcher();
        for (String e : PathUtils.elements(relativePath)){
            m = m.next(e);
        }
        return m.isMatch();
    }

    public PropertyState asPropertyState(){
        List<String> includePatterns = new ArrayList<>(includes.size());
        for (Include i : includes){
            includePatterns.add(i.getPattern());
        }
        return createProperty(META_PROP_PATTERN, includePatterns, STRINGS);
    }

    public Matcher createMatcher(){
        List<Matcher> matchers = Lists.newArrayListWithCapacity(includes.size());
        for(Include include : includes){
            matchers.add(include.createMatcher());
        }
        return CompositeMatcher.compose(matchers);
    }

    @Override
    public String toString() {
        return includes.toString();
    }

}
