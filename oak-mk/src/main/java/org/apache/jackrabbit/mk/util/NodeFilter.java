/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mk.json.JsopTokenizer;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code NodeFilter} represents a filter on property and/or node names specified
 * in JSON format. It allows to specify glob patterns for names of nodes and/or
 * properties to be included or excluded.
 * <p/>
 * Example:
 * <pre>
 * {
 *   "nodes": [ "foo*", "-foo1" ],
 *   "properties": [ "*", "-:childNodeCount" ]
 * }
 * </pre>
 *
 * @see NameFilter
 * @see org.apache.jackrabbit.mk.api.MicroKernel#getNodes(String, String, int, long, int, String)
 */
public class NodeFilter {

    private final NameFilter nodeFilter;
    private final NameFilter propFilter;

    private NodeFilter(NameFilter nodeFilter, NameFilter propFilter) {
        this.nodeFilter = nodeFilter;
        this.propFilter = propFilter;
    }

    public static NodeFilter parse(String json) {
        // parse json format filter
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');

        NameFilter nodeFilter = null, propFilter = null;

        do {
            String type = t.readString();
            t.read(':');
            String[] globs = parseArray(t);
            if (type.equals("nodes")) {
                nodeFilter = new NameFilter(globs);
            } else if (type.equals("properties")) {
                propFilter = new NameFilter(globs);
            } else {
                throw new IllegalArgumentException("illegal filter format");
            }
        } while (t.matches(','));
        t.read('}');

        return new NodeFilter(nodeFilter, propFilter);
    }

    private static String[] parseArray(JsopTokenizer t) {
        List<String> l = new ArrayList<String>();
        t.read('[');
        do {
            l.add(t.readString());
        } while (t.matches(','));
        t.read(']');
        return l.toArray(new String[l.size()]);
    }

    public NameFilter getChildNodeFilter() {
        return nodeFilter;
    }

    public NameFilter getPropertyFilter() {
        return propFilter;
    }

    public boolean includeNode(String name) {
        return nodeFilter == null || nodeFilter.matches(name);
    }

    public boolean includeProperty(String name) {
        return propFilter == null || propFilter.matches(name);
    }
}