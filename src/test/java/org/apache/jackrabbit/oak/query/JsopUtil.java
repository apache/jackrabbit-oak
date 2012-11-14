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
package org.apache.jackrabbit.oak.query;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.List;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.TypeCodes;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;

import com.google.common.collect.Lists;

/**
 * Utility class for working with jsop string diffs
 * 
 */
public class JsopUtil {

    private JsopUtil() {

    }

    /**
     * Applies the commit string to a given Root instance
     * 
     * 
     * The commit string represents a sequence of operations, jsonp style:
     * 
     * <p>
     * / + "test": { "a": { "id": "ref:123" }, "b": { "id" : "str:123" }}
     * <p>
     * or
     * <p>
     * "/ - "test"
     * </p>
     * 
     * @param root
     * @param commit the commit string
     * @throws UnsupportedOperationException if the operation is not supported
     */
    public static void apply(Root root, String commit)
            throws UnsupportedOperationException {
        int index = commit.indexOf(' ');
        String path = commit.substring(0, index).trim();
        Tree c = root.getTree(path);
        if (c == null) {
            // TODO create intermediary?
            throw new UnsupportedOperationException("Non existing path " + path);
        }
        commit = commit.substring(index);
        JsopTokenizer tokenizer = new JsopTokenizer(commit);
        if (tokenizer.matches('-')) {
            removeTree(c, tokenizer);
        } else if (tokenizer.matches('+')) {
            addTree(c, tokenizer);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported " + (char) tokenizer.read() + 
                    ". This should be either '+' or '-'.");
        }
    }

    private static void removeTree(Tree t, JsopTokenizer tokenizer) {
        String path = tokenizer.readString();
        for (String p : PathUtils.elements(path)) {
            if (!t.hasChild(p)) {
                return;
            }
            t = t.getChild(p);
        }
        t.remove();
    }

    private static void addTree(Tree t, JsopTokenizer tokenizer) {
        do {
            String key = tokenizer.readString();
            tokenizer.read(':');
            if (tokenizer.matches('{')) {
                Tree c = t.addChild(key);
                if (!tokenizer.matches('}')) {
                    addTree(c, tokenizer);
                    tokenizer.read('}');
                }
            } else if (tokenizer.matches('[')) {
                t.setProperty(readArrayProperty(key, tokenizer));
            } else {
                t.setProperty(readProperty(key, tokenizer));
            }
        } while (tokenizer.matches(','));
    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private static PropertyState readProperty(String name, JsopReader reader) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            return createProperty(name, number, PropertyType.LONG);
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    throw new UnsupportedOperationException();
                } else {
                    return createProperty(name, value, type);
                }
            } else {
                return StringPropertyState.stringProperty(name, jsonString);
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private static PropertyState readArrayProperty(String name, JsopReader reader) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                type = PropertyType.LONG;
                values.add(Conversions.convert(number).toLong());
            } else if (reader.matches(JsopReader.TRUE)) {
                type = PropertyType.BOOLEAN;
                values.add(true);
            } else if (reader.matches(JsopReader.FALSE)) {
                type = PropertyType.BOOLEAN;
                values.add(false);
            } else if (reader.matches(JsopReader.STRING)) {
                String jsonString = reader.getToken();
                int split = TypeCodes.split(jsonString);
                if (split != -1) {
                    type = TypeCodes.decodeType(split, jsonString);
                    String value = TypeCodes.decodeName(split, jsonString);
                    if (type == PropertyType.BINARY) {
                        throw new UnsupportedOperationException();
                    } else if(type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if(type == PropertyType.DECIMAL) {
                        values.add(Conversions.convert(value).toDecimal());
                    } else {
                        values.add(value);
                    }
                } else {
                    type = PropertyType.STRING;
                    values.add(jsonString);
                }
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return createProperty(name, values, Type.fromTag(type, true));
    }

}
