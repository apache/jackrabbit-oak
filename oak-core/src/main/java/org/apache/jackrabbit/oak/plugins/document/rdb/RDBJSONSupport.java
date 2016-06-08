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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.StableRevisionComparator;

/**
 * Utilities that provide JSON support on top of the existing
 * {@link JsopTokenizer} support in oak-commons.
 * <p>
 * The result of parsing uses the simplest possible Java representation of the
 * JSON values (see Section 3 of RFC 7159), thus
 * <ul>
 * <li>{@code null}, {@link Boolean#TRUE}, {@link Boolean#FALSE}, {@link Number}
 * , or {@link String}, or
 * <li>a {@link List} of representations, or
 * <li>a {@link Map}, mapping member names to representations.
 * </ul>
 * <p>
 * The boolean parameter of the constructor ({link
 * {@link #RDBJSONSupport(boolean)}) allows changing the default for the maps to
 * use sorted maps using {@link Revision}s as keys, as used internally be the
 * {@link DocumentNodeStore}.
 */
public class RDBJSONSupport {

    private final boolean useRevisionMaps;

    /**
     * @param useRevisionMaps
     *            whether to use revision maps instead of regular
     *            {@link Map}s.
     */
    public RDBJSONSupport(boolean useRevisionMaps) {
        this.useRevisionMaps = useRevisionMaps;
    }

    /**
     * Parses the supplied JSON.
     */
    @Nullable
    public Object parse(@Nonnull String json) {
        return parse(new JsopTokenizer(json));
    }

    /**
     * Parses the supplied JSON.
     */
    @Nullable
    public Object parse(@Nonnull JsopTokenizer json) {
        switch (json.read()) {
            case JsopReader.NULL:
                return null;
            case JsopReader.TRUE:
                return Boolean.TRUE;
            case JsopReader.FALSE:
                return Boolean.FALSE;
            case JsopReader.NUMBER:
                String t = json.getToken();
                try {
                    return Long.parseLong(t);
                }
                catch (NumberFormatException ex) {
                    return Double.parseDouble(t);
                }
            case JsopReader.STRING:
                return json.getToken();
            case '{':
                if (useRevisionMaps) {
                    Map<Revision, Object> map = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
                    while (true) {
                        if (json.matches('}')) {
                            break;
                        }
                        String k = json.readString();
                        if (k == null) {
                            throw new IllegalArgumentException("unexpected null revision");
                        }
                        json.read(':');
                        map.put(Revision.fromString(k), parse(json));
                        json.matches(',');
                    }
                    return map;
                } else {
                    Map<String, Object> map = new HashMap<String, Object>();
                    while (true) {
                        if (json.matches('}')) {
                            break;
                        }
                        String k = json.readString();
                        if (k == null) {
                            throw new IllegalArgumentException("unexpected null key");
                        }
                        json.read(':');
                        map.put(k, parse(json));
                        json.matches(',');
                    }
                    return map;
                }
            case '[':
                List<Object> list = new ArrayList<Object>();
                while (true) {
                    if (json.matches(']')) {
                        break;
                    }
                    list.add(parse(json));
                    json.matches(',');
                }
                return list;
            default:
                throw new IllegalArgumentException(json.readRawValue());
        }
    }

    public static void appendJsonMember(StringBuilder sb, String key, Object value) {
        appendJsonString(sb, key);
        sb.append(":");
        appendJsonValue(sb, value);
    }

    public static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        JsopBuilder.escape(s, sb);
        sb.append('"');
    }

    public static void appendJsonMap(StringBuilder sb, Map<Object, Object> map) {
        sb.append("{");
        boolean needComma = false;
        for (Map.Entry<Object, Object> e : map.entrySet()) {
            if (needComma) {
                sb.append(",");
            }
            appendJsonMember(sb, e.getKey().toString(), e.getValue());
            needComma = true;
        }
        sb.append("}");
    }

    public static void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Number) {
            sb.append(value.toString());
        } else if (value instanceof Boolean) {
            sb.append(value.toString());
        } else if (value instanceof String) {
            appendJsonString(sb, (String) value);
        } else if (value instanceof Map) {
            appendJsonMap(sb, (Map<Object, Object>) value);
        } else {
            throw new IllegalArgumentException("unexpected type: " + value.getClass());
        }
    }
}
