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

package org.apache.jackrabbit.oak.remote.content;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteTree;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.oak.remote.RemoteValue.Supplier;
import org.apache.jackrabbit.oak.remote.filter.Filters;
import org.apache.jackrabbit.util.ISO8601;

import java.io.InputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

class ContentRemoteTree implements RemoteTree {

    private final Tree tree;

    private final int depth;

    private final RemoteTreeFilters filters;

    private final ContentRemoteBinaries contentRemoteBinaries;

    public ContentRemoteTree(Tree tree, int depth, RemoteTreeFilters filters, ContentRemoteBinaries contentRemoteBinaries) {
        this.tree = tree;
        this.depth = depth;
        this.filters = filters;
        this.contentRemoteBinaries = contentRemoteBinaries;
    }

    @Override
    public Map<String, RemoteValue> getProperties() {
        Map<String, RemoteValue> properties = new HashMap<String, RemoteValue>();

        for (PropertyState property : getFilteredProperties()) {
            properties.put(property.getName(), getRemoteValue(property));
        }

        return properties;
    }

    private Iterable<? extends PropertyState> getFilteredProperties() {
        return Iterables.filter(tree.getProperties(), getPropertyFilters());
    }

    private Predicate<? super PropertyState> getPropertyFilters() {
        return new Predicate<PropertyState>() {

            @Override
            public boolean apply(PropertyState property) {
                return new Filters(filters.getPropertyFilters()).matches(property.getName());
            }

        };
    }

    private RemoteValue getRemoteValue(PropertyState property) {
        Type<?> type = property.getType();

        if (type == Type.DATE) {
            return RemoteValue.toDate(getDate(property.getValue(Type.DATE)));
        }

        if (type == Type.DATES) {
            return RemoteValue.toMultiDate(getDates(property.getValue(Type.DATES)));
        }

        if (type == Type.BINARY) {
            return getBinaryRemoteValue(property.getValue(Type.BINARY));
        }

        if (type == Type.BINARIES) {
            return getBinaryRemoteValues(property.getValue(Type.BINARIES));
        }

        if (type == Type.BOOLEAN) {
            return RemoteValue.toBoolean(property.getValue(Type.BOOLEAN));
        }

        if (type == Type.BOOLEANS) {
            return RemoteValue.toMultiBoolean(property.getValue(Type.BOOLEANS));
        }

        if (type == Type.DECIMAL) {
            return RemoteValue.toDecimal(property.getValue(Type.DECIMAL));
        }

        if (type == Type.DECIMALS) {
            return RemoteValue.toMultiDecimal(property.getValue(Type.DECIMALS));
        }

        if (type == Type.DOUBLE) {
            return RemoteValue.toDouble(property.getValue(Type.DOUBLE));
        }

        if (type == Type.DOUBLES) {
            return RemoteValue.toMultiDouble(property.getValue(Type.DOUBLES));
        }

        if (type == Type.LONG) {
            return RemoteValue.toLong(property.getValue(Type.LONG));
        }

        if (type == Type.LONGS) {
            return RemoteValue.toMultiLong(property.getValue(Type.LONGS));
        }

        if (type == Type.NAME) {
            return RemoteValue.toName(property.getValue(Type.NAME));
        }

        if (type == Type.NAMES) {
            return RemoteValue.toMultiName(property.getValue(Type.NAMES));
        }

        if (type == Type.PATH) {
            return RemoteValue.toPath(property.getValue(Type.PATH));
        }

        if (type == Type.PATHS) {
            return RemoteValue.toMultiPath(property.getValue(Type.PATHS));
        }

        if (type == Type.REFERENCE) {
            return RemoteValue.toReference(property.getValue(Type.REFERENCE));
        }

        if (type == Type.REFERENCES) {
            return RemoteValue.toMultiReference(property.getValue(Type.REFERENCES));
        }

        if (type == Type.STRING) {
            return RemoteValue.toText(property.getValue(Type.STRING));
        }

        if (type == Type.STRINGS) {
            return RemoteValue.toMultiText(property.getValue(Type.STRINGS));
        }

        if (type == Type.URI) {
            return RemoteValue.toUri(property.getValue(Type.URI));
        }

        if (type == Type.URIS) {
            return RemoteValue.toMultiUri(property.getValue(Type.URIS));
        }

        if (type == Type.WEAKREFERENCE) {
            return RemoteValue.toWeakReference(property.getValue(Type.WEAKREFERENCE));
        }

        if (type == Type.WEAKREFERENCES) {
            return RemoteValue.toMultiWeakReference(property.getValue(Type.WEAKREFERENCES));
        }

        throw new IllegalArgumentException("unrecognized property type");
    }

    private long getDate(String date) {
        Calendar calendar = ISO8601.parse(date);

        if (calendar == null) {
            throw new IllegalStateException("invalid date format");
        }

        return calendar.getTimeInMillis();
    }

    private Iterable<Long> getDates(Iterable<String> dates) {
        return Iterables.transform(dates, new Function<String, Long>() {

            @Override
            public Long apply(String date) {
                return getDate(date);
            }

        });
    }

    private RemoteValue getBinaryRemoteValue(Blob blob) {
        if (getLength(blob) < filters.getBinaryThreshold()) {
            return RemoteValue.toBinary(getBinary(blob));
        } else {
            return RemoteValue.toBinaryId(getBinaryId(blob));
        }
    }

    private RemoteValue getBinaryRemoteValues(Iterable<Blob> blobs) {
        if (getLength(blobs) < filters.getBinaryThreshold()) {
            return RemoteValue.toMultiBinary(getBinaries(blobs));
        } else {
            return RemoteValue.toMultiBinaryId(getBinaryIds(blobs));
        }
    }

    private long getLength(Blob blob) {
        return blob.length();
    }

    private long getLength(Iterable<Blob> blobs) {
        long length = 0;

        for (Blob blob : blobs) {
            length = length + blob.length();
        }

        return length;
    }

    private Supplier<InputStream> getBinary(final Blob blob) {
        return new Supplier<InputStream>() {

            @Override
            public InputStream get() {
                return blob.getNewStream();
            }

        };
    }

    private Iterable<Supplier<InputStream>> getBinaries(Iterable<Blob> blobs) {
        return Iterables.transform(blobs, new Function<Blob, Supplier<InputStream>>() {

            @Override
            public Supplier<InputStream> apply(Blob blob) {
                return getBinary(blob);
            }

        });
    }

    private String getBinaryId(Blob blob) {
        return contentRemoteBinaries.put(blob);
    }

    private Iterable<String> getBinaryIds(Iterable<Blob> blobs) {
        return Iterables.transform(blobs, new Function<Blob, String>() {

            @Override
            public String apply(Blob blob) {
                return getBinaryId(blob);
            }

        });
    }

    @Override
    public Map<String, RemoteTree> getChildren() {
        Map<String, RemoteTree> children = new HashMap<String, RemoteTree>();

        for (Tree child : getFilteredChildren()) {
            if (depth < filters.getDepth()) {
                children.put(child.getName(), new ContentRemoteTree(child, depth + 1, filters, contentRemoteBinaries));
            } else {
                children.put(child.getName(), null);
            }
        }

        return children;
    }

    private Iterable<Tree> getFilteredChildren() {
        Iterable<Tree> result = tree.getChildren();

        if (filters.getChildrenStart() > 0) {
            result = Iterables.skip(result, filters.getChildrenStart());
        }

        if (filters.getChildrenCount() >= 0) {
            result = Iterables.limit(result, filters.getChildrenCount());
        }

        return Iterables.filter(result, getNodeFilters());
    }

    private Predicate<Tree> getNodeFilters() {
        return new Predicate<Tree>() {

            @Override
            public boolean apply(Tree child) {
                return new Filters(filters.getNodeFilters()).matches(child.getName());
            }

        };
    }

    @Override
    public boolean hasMoreChildren() {
        if (filters.getChildrenCount() < 0) {
            return false;
        }

        int start = filters.getChildrenStart();

        if (start < 0) {
            start = 0;
        }

        int count = filters.getChildrenCount();

        if (count < 0) {
            count = 0;
        }

        int max = start + count;

        return tree.getChildrenCount(max) > max;
    }

}
