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
package org.apache.jackrabbit.oak.kernel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;

/**
 * CoreValueUtil provides methods to convert {@code CoreValue}s to the JSON
 * representation passed to MicroKernel and vice versa.
 */
public class CoreValueMapper {

    public static final Map<Integer, String> TYPE2HINT = new HashMap<Integer, String>();
    private static final Map<String, Integer> HINT2TYPE = new HashMap<String, Integer>();

    static {
        for (int type = PropertyType.UNDEFINED; type <= PropertyType.DECIMAL; type++) {
            String hint = PropertyType.nameFromValue(type).substring(0, 3).toLowerCase(Locale.ENGLISH);
            TYPE2HINT.put(type, hint);
            HINT2TYPE.put(hint, type);
        }
    }

    /**
     * Avoid instantiation.
     */
    private CoreValueMapper() {
    }

    public static CoreValue fromJsopReader(JsopReader reader, MicroKernel kernel) {
        return fromJsopReader(reader, new CoreValueFactoryImpl(kernel));
    }

    /**
     * Read a single value from the specified reader and convert it into a
     * {@code CoreValue}. This method takes type-hint prefixes into account.
     *
     * @param reader The JSON reader.
     * @param factory The factory used to create the value.
     * @return The value such as defined by the token obtained from the reader.
     */
    public static CoreValue fromJsopReader(JsopReader reader, CoreValueFactory factory) {
        CoreValue value;
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            value = factory.createValue(Long.valueOf(number));
        } else if (reader.matches(JsopReader.TRUE)) {
            value = factory.createValue(true);
        } else if (reader.matches(JsopReader.FALSE)) {
            value = factory.createValue(false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            if (startsWithHint(jsonString)) {
                int type = HINT2TYPE.get(jsonString.substring(0, 3));
                value = factory.createValue(jsonString.substring(4), type);
            } else {
                value = factory.createValue(jsonString);
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
     * @param kernel The Microkernel instance from which the values originate
     * @return A list of values such as defined by the reader.
     */
    public static List<CoreValue> listFromJsopReader(JsopReader reader, MicroKernel kernel) {
        List<CoreValue> values = new ArrayList<CoreValue>();
        while (!reader.matches(']')) {
            values.add(fromJsopReader(reader, kernel));
            reader.matches(',');
        }
        return values;
    }

    /**
     * Returns {@code true} if the specified JSON String represents a value
     * serialization that includes a leading type hint.
     *
     * @param jsonString The JSON String representation of a {@code CoreValue}
     * @return {@code true} if the {@code jsonString} starts with a type
     * hint; {@code false} otherwise.
     * @see #buildJsonStringWithHint(org.apache.jackrabbit.oak.api.CoreValue)
     */
    public static boolean startsWithHint(String jsonString) {
        return jsonString.length() >= 4 && jsonString.charAt(3) == ':';
    }

}