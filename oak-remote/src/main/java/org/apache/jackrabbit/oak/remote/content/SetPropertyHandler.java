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
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteValue.Supplier;
import org.apache.jackrabbit.oak.remote.RemoteValue.TypeHandler;
import org.apache.jackrabbit.util.ISO8601;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

class SetPropertyHandler extends TypeHandler {

    private final ContentRemoteBinaries binaries;

    private final Root root;

    private final Tree tree;

    private final String name;

    public SetPropertyHandler(ContentRemoteBinaries binaries, Root root, Tree tree, String name) {
        this.binaries = binaries;
        this.root = root;
        this.tree = tree;
        this.name = name;
    }

    @Override
    public void isBinary(Supplier<InputStream> value) {
        tree.setProperty(name, getBlob(root, value), Type.BINARY);
    }

    @Override
    public void isMultiBinary(Iterable<Supplier<InputStream>> value) {
        tree.setProperty(name, getBlobs(root, value), Type.BINARIES);
    }

    @Override
    public void isBinaryId(String value) {
        tree.setProperty(name, getBlobFromId(binaries, value), Type.BINARY);
    }

    @Override
    public void isMultiBinaryId(Iterable<String> value) {
        tree.setProperty(name, getBlobsFromIds(binaries, value), Type.BINARIES);
    }

    @Override
    public void isBoolean(Boolean value) {
        tree.setProperty(name, value, Type.BOOLEAN);
    }

    @Override
    public void isMultiBoolean(Iterable<Boolean> value) {
        tree.setProperty(name, value, Type.BOOLEANS);
    }

    @Override
    public void isDate(Long value) {
        tree.setProperty(name, getDate(value), Type.DATE);
    }

    @Override
    public void isMultiDate(Iterable<Long> value) {
        tree.setProperty(name, getDates(value), Type.DATES);
    }

    @Override
    public void isDecimal(BigDecimal value) {
        tree.setProperty(name, value, Type.DECIMAL);
    }

    @Override
    public void isMultiDecimal(Iterable<BigDecimal> value) {
        tree.setProperty(name, value, Type.DECIMALS);
    }

    @Override
    public void isDouble(Double value) {
        tree.setProperty(name, value, Type.DOUBLE);
    }

    @Override
    public void isMultiDouble(Iterable<Double> value) {
        tree.setProperty(name, value, Type.DOUBLES);
    }

    @Override
    public void isLong(Long value) {
        tree.setProperty(name, value, Type.LONG);
    }

    @Override
    public void isMultiLong(Iterable<Long> value) {
        tree.setProperty(name, value, Type.LONGS);
    }

    @Override
    public void isName(String value) {
        tree.setProperty(name, value, Type.NAME);
    }

    @Override
    public void isMultiName(Iterable<String> value) {
        tree.setProperty(name, value, Type.NAMES);
    }

    @Override
    public void isPath(String value) {
        tree.setProperty(name, value, Type.PATH);
    }

    @Override
    public void isMultiPath(Iterable<String> value) {
        tree.setProperty(name, value, Type.PATHS);
    }

    @Override
    public void isReference(String value) {
        tree.setProperty(name, value, Type.REFERENCE);
    }

    @Override
    public void isMultiReference(Iterable<String> value) {
        tree.setProperty(name, value, Type.REFERENCES);
    }

    @Override
    public void isText(String value) {
        tree.setProperty(name, value, Type.STRING);
    }

    @Override
    public void isMultiText(Iterable<String> value) {
        tree.setProperty(name, value, Type.STRINGS);
    }

    @Override
    public void isUri(String value) {
        tree.setProperty(name, value, Type.URI);
    }

    @Override
    public void isMultiUri(Iterable<String> value) {
        tree.setProperty(name, value, Type.URIS);
    }

    @Override
    public void isWeakReference(String value) {
        tree.setProperty(name, value, Type.WEAKREFERENCE);
    }

    @Override
    public void isMultiWeakReference(Iterable<String> value) {
        tree.setProperty(name, value, Type.WEAKREFERENCES);
    }

    private Blob getBlob(Root root, Supplier<InputStream> supplier) {
        InputStream inputStream = supplier.get();

        if (inputStream == null) {
            throw new IllegalStateException("invalid input stream");
        }

        Blob blob;

        try {
            blob = root.createBlob(inputStream);
        } catch (Exception e) {
            throw new IllegalStateException("unable to create a blob", e);
        }

        return blob;
    }

    private Iterable<Blob> getBlobs(final Root root, Iterable<Supplier<InputStream>> suppliers) {
        return Iterables.transform(suppliers, new Function<Supplier<InputStream>, Blob>() {

            @Override
            public Blob apply(Supplier<InputStream> supplier) {
                return getBlob(root, supplier);
            }

        });
    }

    private Blob getBlobFromId(ContentRemoteBinaries binaries, String binaryId) {
        return binaries.get(binaryId);
    }

    private Iterable<Blob> getBlobsFromIds(final ContentRemoteBinaries binaries, Iterable<String> binaryIds) {
        return Iterables.transform(binaryIds, new Function<String, Blob>() {

            @Override
            public Blob apply(String binaryId) {
                return getBlobFromId(binaries, binaryId);
            }

        });
    }

    private String getDate(Long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return ISO8601.format(calendar);
    }

    private Iterable<String> getDates(Iterable<Long> times) {
        return Iterables.transform(times, new Function<Long, String>() {

            @Override
            public String apply(Long time) {
                return getDate(time);
            }

        });
    }

}
