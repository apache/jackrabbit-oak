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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_BUNDLING_PATH;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_PATTERN;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BundlorUtilsTest {
    @Test
    public void matchingProperties_Simple() throws Exception{
        Map<String, PropertyState> result = BundlorUtils.getMatchingProperties(
            create("a", "b", "c"), Matcher.NON_MATCHING
        );

        assertTrue(result.isEmpty());
    }

    @Test
    public void matchingProperties_BaseLevel() throws Exception{
        Matcher m = new Include("jcr:content").createMatcher();
        Map<String, PropertyState> result = BundlorUtils.getMatchingProperties(
                create("a",
                        concat("jcr:content", META_PROP_BUNDLING_PATH),
                        "jcr:content/jcr:data",
                        "jcr:primaryType",
                        META_PROP_PATTERN
                ), m
        );
        assertThat(result.keySet(), hasItems("a", "jcr:primaryType", META_PROP_PATTERN));
    }

    @Test
    public void matchingProperties_FirstLevel() throws Exception{
        Matcher m = new Include("jcr:content").createMatcher().next("jcr:content");
        Map<String, PropertyState> result = BundlorUtils.getMatchingProperties(
                create("a",
                        concat("jcr:content", META_PROP_BUNDLING_PATH),
                        "jcr:content/jcr:data",
                        "jcr:content/metadata/format",
                        "jcr:primaryType",
                        META_PROP_PATTERN
                ), m
        );
        assertThat(result.keySet(), hasItems("jcr:data"));
        assertEquals("jcr:data", result.get("jcr:data").getName());
    }

    @Test
    public void childNodeNames() throws Exception{
        List<String> testData = asList("x",
                concat("jcr:content", META_PROP_BUNDLING_PATH),
                "jcr:content/jcr:data",
                concat("jcr:content/metadata", META_PROP_BUNDLING_PATH),
                "jcr:content/metadata/format",
                concat("jcr:content/comments", META_PROP_BUNDLING_PATH),
                concat("jcr:content/renditions/original", META_PROP_BUNDLING_PATH)
        );

        Matcher m = new Include("jcr:content/*").createMatcher();
        Set<String> names = BundlorUtils.getChildNodeNames(testData, m);
        assertThat(names, hasItem("jcr:content"));

        names = BundlorUtils.getChildNodeNames(testData, m.next("jcr:content"));
        assertThat(names, hasItems("metadata", "comments"));

        names = BundlorUtils.getChildNodeNames(testData, m.next("jcr:content").next("metadata"));
        assertTrue(names.isEmpty());
    }

    private Map<String, PropertyState> create(String ... keyNames){
        Map<String, PropertyState> map = new HashMap<>();
        for (String key : keyNames){
            map.put(key, PropertyStates.createProperty(key, Boolean.TRUE));
        }
        return map;
    }

}