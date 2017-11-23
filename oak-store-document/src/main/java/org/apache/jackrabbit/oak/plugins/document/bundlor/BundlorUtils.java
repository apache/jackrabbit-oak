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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_BUNDLING_PATH;

public final class BundlorUtils {
    public static final Predicate<PropertyState> NOT_BUNDLOR_PROPS = new Predicate<PropertyState>() {
        @Override
        public boolean apply(PropertyState input) {
            return !input.getName().startsWith(DocumentBundlor.BUNDLOR_META_PROP_PREFIX);
        }
    };


    public static Map<String, PropertyState> getMatchingProperties(Map<String, PropertyState> props, Matcher matcher){
        if (!matcher.isMatch()){
            return Collections.emptyMap();
        }

        Map<String, PropertyState> result = Maps.newHashMap();
        for (Map.Entry<String, PropertyState> e : props.entrySet()){
            String propertyPath = e.getKey();

            //PathUtils.depth include depth for property name. So
            //reduce 1 to get node depth
            int depth = PathUtils.getDepth(propertyPath) - 1;

            if (!propertyPath.startsWith(matcher.getMatchedPath())){
                continue;
            }

            if (depth != matcher.depth()){
                continue;
            }

            //Extract property name from relative property path
            final String newKey = PathUtils.getName(propertyPath);
            PropertyState value = e.getValue();

            if (depth > 0){
                value = new PropertyStateWrapper(value){
                    @Nonnull
                    @Override
                    public String getName() {
                        return newKey;
                    }
                };
            }

            result.put(newKey, value);
        }
        return result;
    }

    public static Set<String> getChildNodeNames(Collection<String> keys, Matcher matcher){
        Set<String> childNodeNames = Sets.newHashSet();

        //Immediate child should have depth 1 more than matcher depth
        int expectedDepth = matcher.depth() + 1;

        for (String key : keys){
            List<String> elements = ImmutableList.copyOf(PathUtils.elements(key));
            int depth = elements.size() - 1;

            if (depth == expectedDepth
                    && key.startsWith(matcher.getMatchedPath())
                    && elements.get(elements.size() - 1).equals(META_PROP_BUNDLING_PATH)){
                //Child node name is the second last element
                //[jcr:content/:self -> [jcr:content, :self]
                childNodeNames.add(elements.get(elements.size() - 2));
            }
        }
        return childNodeNames;
    }

    static boolean isBundlingRoot(NodeState state){
        return state.hasProperty(DocumentBundlor.META_PROP_PATTERN);
    }

    static boolean isBundledChild(NodeState state){
        return state.hasProperty(DocumentBundlor.META_PROP_BUNDLING_PATH);
    }

    static boolean isBundledNode(NodeState state){
        return isBundlingRoot(state) || isBundledChild(state);
    }
}
