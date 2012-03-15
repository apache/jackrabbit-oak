/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@code JsonValue} represents either an {@link JsonArray array}, an
 * {@link JsonObject object} or a {@link JsonAtom primitive value} (atom)
 * of a JSON document.
 */
public abstract class JsonValue {
    public enum Type {
        STRING(false),
        NUMBER(false),
        BOOLEAN(false),
        NULL(false),
        OBJECT(true),
        ARRAY(true);

        private final boolean compound;

        Type(boolean compound) {
            this.compound = compound;
        }

        /**
         * @return {@code true} for {@link Type#ARRAY} and {@link Type#OBJECT},
         * {@code false} otherwise.
         */
        public boolean compound() {
            return compound;
        }
    }

    /**
     * Visitor for dispatching compound {@code JsonValue}s.
     */
    public abstract static class Visitor {
        public void visit(JsonAtom atom) { }
        public void visit(JsonArray array) { }
        public void visit(JsonObject object) { }
    }

    /**
     * Convert {@code jsonValue} to its JSON representation.
     * @param jsonValue
     * @return a JSON representation of {@code jsonValue}
     */
    public static String toJson(JsonValue jsonValue) {
        final StringBuilder sb = new StringBuilder();
        jsonValue.accept(new Visitor() {
            @Override
            public void visit(JsonAtom atom) {
                sb.append(toJson(atom));
            }

            @Override
            public void visit(JsonArray array) {
                sb.append('[');
                String comma = "";
                for (JsonValue value : array.value()) {
                    sb.append(comma);
                    comma = ",";
                    value.accept(this);
                }
                sb.append(']');
            }

            @Override
            public void visit(JsonObject object) {
                sb.append('{');
                String comma = "";
                for (Entry<String, JsonValue> entry : object.value().entrySet()) {
                    sb.append(comma);
                    comma = ",";
                    sb.append(quote(entry.getKey())).append(':');
                    entry.getValue().accept(this);
                }
                sb.append('}');
            }
        });
        return sb.toString();
    }

    /**
     * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters
     * (U+0000 through U+001F) in {@code text}.
     * @param text
     * @return {@code text} with control characters escaped
     */
    public static String escape(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    //Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if (ch >= '\u0000' && ch <= '\u001F' ||
                        ch >= '\u007F' && ch <= '\u009F' ||
                        ch >= '\u2000' && ch <= '\u20FF') {

                        String ss = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int k = 0; k < 4 - ss.length(); k++) {
                            sb.append('0');
                        }
                        sb.append(ss.toUpperCase());
                    }
                    else {
                        sb.append(ch);
                    }
            }
        }

        return sb.toString();
    }

    /**
     * Unescape escaped control characters in {@code text}
     * @param text
     * @return {@code text} with control characters escaped
     * @throws StringIndexOutOfBoundsException  on unterminated escape sequences
     * @throws NumberFormatException  on invalid escape sequences
     */
    public static String unescape(String text) {
        if (text.isEmpty()) {
            return text;
        }

        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < text.length(); k++) {
            char c = text.charAt(k);
            if (c == '\\') {
                c = text.charAt(++k);
                switch (c) {
                    case 'b':
                        sb.append('\b');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'f':
                        sb.append('\f');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 'u':
                        String u = text.substring(++k, k += 4);
                        sb.append((char) Integer.parseInt(u, 16));
                        break;
                    case 'x':
                        String x = text.substring(++k, k += 2);
                        sb.append((char) Integer.parseInt(x, 16));
                        break;
                    default:
                        sb.append(c);
                }
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * @return the value carried by this {@code JsonValue}
     */
    public abstract Object value();

    /**
     * @return the type of this {@code JsonValue}
     */
    public abstract Type type();

    /**
     * Dispatch this {@code JsonValue} using {@code visitor}
     * @param visitor
     */
    public abstract void accept(Visitor visitor);

    /**
     * @return {@code true} iff {@code this} is an instance of {@code JsonAtom}
     */
    public boolean isAtom() {
        return !type().compound();
    }

    /**
     * @return {@code true} iff {@code this} is an instance of {@code JsonArray}
     */
    public boolean isArray() {
        return type() == Type.ARRAY;
    }

    /**
     * @return {@code true} iff {@code this} is an instance of {@code JsonObject}
     */
    public boolean isObject() {
        return type() == Type.OBJECT;
    }

    /**
     * @return {@code true} iff {@code this} represents a JSON {@code null} value
     */
    public boolean isNull() {
        return this == JsonAtom.NULL || equals(JsonAtom.NULL);
    }

    /**
     * @return {@code true} iff {@code this} represents a JSON {@code true} value
     */
    public boolean isTrue() {
        return this == JsonAtom.TRUE || equals(JsonAtom.TRUE);
    }

    /**
     * @return {@code true} iff {@code this} represents a JSON {@code false} value
     */
    public boolean isFalse() {
        return this == JsonAtom.FALSE || equals(JsonAtom.FALSE);
    }

    /**
     * @return {@code this} as {@code JsonAtom}
     * @throws UnsupportedOperationException if {@code this} is not an instance of {@code JsonAtom}
     */
    public JsonAtom asAtom() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@code this} as {@code JsonArray}
     * @throws UnsupportedOperationException if {@code this} is not an instance of {@code JsonArray}
     */
    public JsonArray asArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@code this} as {@code JsonObject}
     * @throws UnsupportedOperationException if {@code this} is not an instance of {@code JsonObject}
     */
    public JsonObject asObject() {
        throw new UnsupportedOperationException();
    }

    /**
     * Convert this {@code JsonValue} to its JSON representation.
     * @return a JSON representation of this {@code JsonValue}
     */
    public String toJson() {
        return toJson(this);
    }

    /**
     * This class represents primitive JSON values (atoms). These are values of type
     * {@link Type#STRING} {@link Type#NUMBER} {@link Type#BOOLEAN} and {@link Type#NULL}.
     */
    public static class JsonAtom extends JsonValue {
        public static final JsonAtom NULL = new JsonAtom("null", Type.NULL);
        public static final JsonAtom TRUE = new JsonAtom("true", Type.BOOLEAN);
        public static final JsonAtom FALSE = new JsonAtom("false", Type.BOOLEAN);

        private final String value;
        private final Type type;

        public JsonAtom(String value, Type type) {
            this.value = value;
            this.type = type;
        }

        public static JsonAtom string(String value) {
            return new JsonAtom(value, Type.STRING);
        }

        public static JsonAtom number(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new IllegalArgumentException(Double.toString(value));
            }
            return new JsonAtom(Double.toString(value), Type.NUMBER);
        }

        public static JsonAtom number(long value) {
            return new JsonAtom(Long.toString(value), Type.NUMBER);
        }

        public static JsonAtom number(BigDecimal value) {
            return new JsonAtom(value.toString(), Type.NUMBER);
        }

        /**
         * Create a new {@code JsonAtom} from {@code token}.
         * @param token
         * @throws IllegalArgumentException  if {@code token} does not represent
         * an primitive type (atom).
         */
        public JsonAtom(Token token) {
            this(token.text(), valueType(token.type()));
        }

        @Override
        public JsonAtom asAtom() {
            return this;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public String toString() {
            return value + ": " + type;
        }

        @Override
        public int hashCode() {
            return 37 * (37 * (17 + value().hashCode()) + type().hashCode());
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof JsonAtom) {
                JsonAtom that = (JsonAtom) other;
                return that.value().equals(value()) && that.type() == type();
            }
            else {
                return false;
            }
        }

        //------------------------------------------< private >---

        private static JsonValue.Type valueType(Token.Type type) {
            switch (type) {
                case TRUE:
                case FALSE:
                    return JsonValue.Type.BOOLEAN;
                case NULL:
                    return JsonValue.Type.NULL;
                case STRING:
                    return JsonValue.Type.STRING;
                case NUMBER:
                    return JsonValue.Type.NUMBER;
                default:
                    throw new IllegalArgumentException("Cannot map token type " + type + " to value type");
            }
        }
    }

    /**
     * This class represents JSON arrays.
     */
    public static class JsonArray extends JsonValue {
        public static JsonArray EMPTY = new JsonArray(Collections.<JsonValue>emptyList());
        
        private final List<JsonValue> values;

        public JsonArray(List<JsonValue> values) {
            this.values = values;
        }

        public JsonArray() {
            this(new ArrayList<JsonValue>());
        }

        /**
         * Append {@code value} to the end of this array.
         * @param value
         */
        public void add(JsonValue value) {
            values.add(value);
        }

        /**
         * Removes a value from this array
         * @param value
         * @return  {@code true} iff the array contains {@code value}
         */
        public boolean remove(JsonValue value) {
            return values.remove(value);
        }

        /**
         * @param index
         * @return the {@code JsonValue} at {@code index}.
         * @throws IndexOutOfBoundsException  if {@code index} is out of range
         */
        public JsonValue get(int index) {
            return values.get(index);
        }

        @Override
        public JsonArray asArray() {
            return this;
        }

        @Override
        public List<JsonValue> value() {
            return values;
        }

        @Override
        public Type type() {
            return Type.ARRAY;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Override
        public int hashCode() {
            return value().hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof JsonArray) {
                JsonArray that = (JsonArray) other;
                return that.value().equals(value());
            }
            else {
                return false;
            }
        }
    }

    /**
     * This class represents JSON objects.
     */
    public static class JsonObject extends JsonValue {
        public static final JsonObject EMPTY = new JsonObject(Collections.<String, JsonValue>emptyMap());

        private final Map<String, JsonValue> values;

        public JsonObject(Map<String, JsonValue> values) {
            this.values = values;
        }

        public JsonObject() {
            this(new HashMap<String, JsonValue>());
        }

        /**
         * Put {@code value} into this object
         * @param key
         * @param value
         */
        public void put(String key, JsonValue value) {
            values.put(key, value);
        }

        /**
         * @param key
         * @return  the {@code JsonValue} identified by {@code key} or {@code null}
         * if no value exists for {@code key}.
         */
        public JsonValue get(String key) {
            return values.get(key);
        }

        /**
         * Remove {@code key} from this object
         * @param key
         * @return  the {@code JsonValue} identified by {@code key} or {@code null}
         * if no value exists for {@code key}.
         */
        public JsonValue remove(String key) {
            return values.remove(key);
        }

        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public JsonObject asObject() {
            return this;
        }

        @Override
        public Map<String, JsonValue> value() {
            return values;
        }

        @Override
        public Type type() {
            return Type.OBJECT;
        }

        @Override
        public void accept(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Override
        public int hashCode() {
            return value().hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof JsonObject) {
                JsonObject that = (JsonObject) other;
                return that.value().equals(value());
            }
            else {
                return false;
            }
        }
    }

    //------------------------------------------< private >---

    private static String toJson(JsonAtom atom) {
        return atom.type() == Type.STRING
            ? quote(escape(atom.value()))
            : atom.value();
    }

    private static String quote(String text) {
        return '\"' + text + '\"';
    }

}
