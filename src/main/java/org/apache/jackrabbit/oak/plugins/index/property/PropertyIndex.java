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
package org.apache.jackrabbit.oak.plugins.index.property;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.query.index.TraversingCursor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

public class PropertyIndex implements QueryIndex {

    private static final int MAX_STRING_LENGTH = 100; // TODO: configurable

    static String encode(CoreValue value) {
        try {
            String string = value.getString();
            if (string.length() > MAX_STRING_LENGTH) {
                string.substring(0, MAX_STRING_LENGTH);
            }
            return URLEncoder.encode(string, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
           throw new IllegalStateException("UTF-8 is unsupported", e);
        }
    }


    //--------------------------------------------------------< QueryIndex >--

    @Override
    public String getIndexName() {
        return "oak:index";
    }

    @Override
    public double getCost(Filter filter) {
        return 1.0; // FIXME: proper cost calculation
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        Set<String> paths = null;

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (pr.firstIncluding && pr.lastIncluding
                    && pr.first.equals(pr.last) // TODO: range queries
                    && lookup.isIndexed(pr.propertyName, "/")) { // TODO: path
                Set<String> set = lookup.find(pr.propertyName, pr.first);
                if (paths == null) {
                    paths = Sets.newHashSet(set);
                } else {
                    paths.retainAll(set);
                }
            }
        }

        if (paths != null) {
            return new PathCursor(paths);
        } else {
            return new TraversingCursor(filter, root);
        }
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        return "oak:index"; // TODO: better plans
    }

    private static class PathCursor implements Cursor {

        private final Iterator<String> iterator;

        private String path;

        public PathCursor(Collection<String> paths) {
            this.iterator = paths.iterator();
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                path = iterator.next();
                return true;
            } else {
                path = null;
                return false;
            }
        }

        @Override
        public IndexRow currentRow() {
            // TODO support jcr:score and possibly rep:exceprt
            return new IndexRowImpl(path);
        }

    }

}