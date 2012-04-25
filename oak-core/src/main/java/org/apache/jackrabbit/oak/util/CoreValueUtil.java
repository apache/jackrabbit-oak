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
 * TODO: review if this should be added to CoreValue/*Factory interfaces
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
                    jsonString = buildJsonStringWithType(value);
                } else {
                    jsonString = JsonBuilder.encode(value.getString());
                }
                break;
            default:
                // any other type
                jsonString = buildJsonStringWithType(value);
        }
        return jsonString;
    }

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

    public static List<CoreValue> listFromJsopReader(JsopReader reader, CoreValueFactory valueFactory) {
        if (!reader.matches('[')) {
            List<CoreValue> values = new ArrayList<CoreValue>();
            while (!reader.matches(']')) {
                values.add(fromJsopReader(reader, valueFactory));
                reader.matches(',');
            }
            return values;
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    private static String buildJsonStringWithType(CoreValue value) {
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