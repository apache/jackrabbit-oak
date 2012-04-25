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
package org.apache.jackrabbit.oak.util;

import org.apache.jackrabbit.mk.json.JsonBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CoreValueUtil...
 *
 * TODO: review if this should be added to CoreValue/*Factory interfaces/implementation
 */
public class CoreValueUtil {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(CoreValueUtil.class);

    private static final Map<Integer, String> TYPE2HINT = new HashMap<Integer, String>();
    private static final Map<String, Integer> HINT2TYPE = new HashMap<String, Integer>();
    static {
        for (int type = PropertyType.UNDEFINED; type <= PropertyType.DECIMAL; type++) {
            String hint = PropertyType.nameFromValue(type).substring(0,3).toLowerCase();
            TYPE2HINT.put(type, hint);
            HINT2TYPE.put(hint, type);
        }
    }

    /**
     * Returns the internal JSON representation of the specified {@code value}
     * that is stored in the MicroKernel. All property types that are not
     * reflected as JSON types are converted to strings and get a type prefix.
     *
     * @param value The core value to be converted.
     * @return The encoded JSON string.
     * @see JsonBuilder#encode(String)
     * @see JsonBuilder#encode(long)
     * @see JsonBuilder#encode(long)
     */
    public static String toJsonValue(CoreValue value) {
        String jsonString;
        switch (value.getType()) {
            case PropertyType.BOOLEAN:
                jsonString = JsonBuilder.encode(value.getBoolean());
                break;
            case PropertyType.LONG:
                jsonString = JsonBuilder.encode(value.getLong());
                break;
            case PropertyType.STRING:
                String str = value.getString();
                if (startsWithHint(str)) {
                    jsonString = buildJsonStringWithHint(value);
                } else {
                    jsonString = JsonBuilder.encode(value.getString());
                }
                break;
            default:
                // any other type
                jsonString = buildJsonStringWithHint(value);
        }
        return jsonString;
    }

    /**
     * Returns an JSON array containing the JSON representation of the
     * specified values.
     *
     * @param values
     * @return JSON array containing the JSON representation of the specified
     * values.
     * @see #toJsonValue(org.apache.jackrabbit.oak.api.CoreValue)
     */
    public static String toJsonArray(Iterable<CoreValue> values) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (CoreValue cv : values) {
            sb.append(toJsonValue(cv));
            sb.append(',');
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Read a single value from the specified reader and convert it into a
     * {@code CoreValue}. This method takes type-hint prefixes into account.
     *
     * @param reader The JSON reader.
     * @param valueFactory The factory used to create the value.
     * @return The value such as defined by the token obtained from the reader.
     */
    public static CoreValue fromJsopReader(JsopReader reader, CoreValueFactory valueFactory) {
        CoreValue value;
        if (reader.matches(JsopTokenizer.NUMBER)) {
            String number = reader.getToken();
            value = valueFactory.createValue(Long.valueOf(number));
        } else if (reader.matches(JsopTokenizer.TRUE)) {
            value = valueFactory.createValue(true);
        } else if (reader.matches(JsopTokenizer.FALSE)) {
            value = valueFactory.createValue(false);
        } else if (reader.matches(JsopTokenizer.STRING)) {
            String jsonString = reader.getToken();
            if (startsWithHint(jsonString)) {
                int type = HINT2TYPE.get(jsonString.substring(0,3));
                value = valueFactory.createValue(jsonString.substring(4), type);
            } else {
                value = valueFactory.createValue(jsonString);
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
        return value;
    }

    /**
     * Read the list of values from the specified reader and convert them into
     * {@link CoreValue}s. This method takes type-hint prefixes into account.
     *
     * @param reader The JSON reader.
     * @param valueFactory The factory used to create the values.
     * @return A list of values such as defined by the reader.
     */
    public static List<CoreValue> listFromJsopReader(JsopReader reader, CoreValueFactory valueFactory) {
        List<CoreValue> values = new ArrayList<CoreValue>();
        while (!reader.matches(']')) {
            values.add(fromJsopReader(reader, valueFactory));
            reader.matches(',');
        }
        return values;
    }

    private static String buildJsonStringWithHint(CoreValue value) {
        StringBuilder sb = new StringBuilder();
        sb.append(TYPE2HINT.get(value.getType()));
        sb.append(':');
        sb.append(value.getString());
        return JsonBuilder.encode(sb.toString());
    }

    private static boolean startsWithHint(String jsonString) {
        return jsonString.length() >= 4 && jsonString.charAt(3) == ':';
    }
}