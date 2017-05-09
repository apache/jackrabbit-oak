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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteResult;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.util.ISO8601;

import javax.jcr.PropertyType;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

class ContentRemoteResult implements RemoteResult {

    private final ContentRemoteBinaries binaries;

    private final ResultRow row;

    public ContentRemoteResult(ContentRemoteBinaries binaries, ResultRow row) {
        this.binaries = binaries;
        this.row = row;
    }

    @Override
    public RemoteValue getColumnValue(String column) {
        return toRemoteValue(row.getValue(column));
    }

    private RemoteValue toRemoteValue(PropertyValue value) {
        if (value == null) {
            return null;
        }
        
        Type<?> type = value.getType();

        if (type.isArray()) {
            return toMultiRemoteValue(value);
        } else {
            return toSingleRemoteValue(value);
        }
    }

    private RemoteValue toSingleRemoteValue(PropertyValue value) {
        Type<?> type = value.getType();

        switch (type.tag()) {
            case PropertyType.STRING:
                return RemoteValue.toText(value.getValue(Type.STRING));
            case PropertyType.BINARY:
                return RemoteValue.toBinaryId(binaries.put(value.getValue(Type.BINARY)));
            case PropertyType.LONG:
                return RemoteValue.toLong(value.getValue(Type.LONG));
            case PropertyType.DOUBLE:
                return RemoteValue.toDouble(value.getValue(Type.DOUBLE));
            case PropertyType.DATE:
                return RemoteValue.toDate(ISO8601.parse(value.getValue(Type.DATE)).getTimeInMillis());
            case PropertyType.BOOLEAN:
                return RemoteValue.toBoolean(value.getValue(Type.BOOLEAN));
            case PropertyType.NAME:
                return RemoteValue.toName(value.getValue(Type.NAME));
            case PropertyType.PATH:
                return RemoteValue.toPath(value.getValue(Type.PATH));
            case PropertyType.REFERENCE:
                return RemoteValue.toReference(value.getValue(Type.REFERENCE));
            case PropertyType.WEAKREFERENCE:
                return RemoteValue.toWeakReference(value.getValue(Type.WEAKREFERENCE));
            case PropertyType.URI:
                return RemoteValue.toUri(value.getValue(Type.URI));
            case PropertyType.DECIMAL:
                return RemoteValue.toDecimal(value.getValue(Type.DECIMAL));
        }

        throw new IllegalStateException("type not supported");
    }

    private RemoteValue toMultiRemoteValue(PropertyValue value) {
        Type<?> type = value.getType();

        switch (type.tag()) {
            case PropertyType.STRING:
                return RemoteValue.toMultiText(value.getValue(Type.STRINGS));
            case PropertyType.BINARY:
                return RemoteValue.toMultiBinaryId(readBinaryValues(value));
            case PropertyType.LONG:
                return RemoteValue.toMultiLong(value.getValue(Type.LONGS));
            case PropertyType.DOUBLE:
                return RemoteValue.toMultiDouble(value.getValue(Type.DOUBLES));
            case PropertyType.DATE:
                return RemoteValue.toMultiDate(readDateValues(value));
            case PropertyType.BOOLEAN:
                return RemoteValue.toMultiBoolean(value.getValue(Type.BOOLEANS));
            case PropertyType.NAME:
                return RemoteValue.toMultiName(value.getValue(Type.NAMES));
            case PropertyType.PATH:
                return RemoteValue.toMultiPath(value.getValue(Type.PATHS));
            case PropertyType.REFERENCE:
                return RemoteValue.toMultiReference(value.getValue(Type.REFERENCES));
            case PropertyType.WEAKREFERENCE:
                return RemoteValue.toMultiWeakReference(value.getValue(Type.WEAKREFERENCES));
            case PropertyType.URI:
                return RemoteValue.toMultiUri(value.getValue(Type.URIS));
            case PropertyType.DECIMAL:
                return RemoteValue.toMultiDecimal(value.getValue(Type.DECIMALS));
        }

        throw new IllegalStateException("type not supported");
    }

    private Iterable<String> readBinaryValues(PropertyValue value) {
        List<String> result = newArrayList();

        for (Blob blob : value.getValue(Type.BINARIES)) {
            result.add(binaries.put(blob));
        }

        return result;
    }

    private Iterable<Long> readDateValues(PropertyValue value) {
        List<Long> result = newArrayList();

        for (String string : value.getValue(Type.DATES)) {
            result.add(ISO8601.parse(string).getTimeInMillis());
        }

        return result;
    }

    @Override
    public String getSelectorPath(String selector) {
        return row.getPath(selector);
    }

}
