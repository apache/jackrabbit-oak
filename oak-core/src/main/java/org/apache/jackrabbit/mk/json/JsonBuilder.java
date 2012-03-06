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

package org.apache.jackrabbit.mk.json;

import java.io.IOException;

/**
 * Partially based on json-simple
 * Limitation: arrays can only have primitive members (i.e. no arrays nor objects)
 */
public final class JsonBuilder {
    final Appendable writer;

    private JsonBuilder(Appendable writer) {
        this.writer = writer;
    }

    public static JsonObjectBuilder create(Appendable writer) throws IOException {
        return new JsonBuilder(writer).new JsonObjectBuilder(null);
    }

    public final class JsonObjectBuilder {
        private final JsonObjectBuilder parent;

        private boolean hasKeys;

        public JsonObjectBuilder(JsonObjectBuilder parent) throws IOException {
            this.parent = parent;
            writer.append('{');
        }

        public JsonObjectBuilder value(String key, String value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder valueEncoded(String key, String value) throws IOException {
            write(key, value);
            return this;
        }

        public JsonObjectBuilder value(String key, int value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder value(String key, long value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder value(String key, float value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder value(String key, double value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder value(String key, Number value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder value(String key, boolean value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder nil(String key) throws IOException {
            write(key, "null");
            return this;
        }

        public JsonObjectBuilder array(String key, String[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, int[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, long[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, float[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, double[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, Number[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonObjectBuilder array(String key, boolean[] value) throws IOException {
            write(key, encode(value));
            return this;
        }

        public JsonArrayBuilder array(String key) throws IOException {
            writeKey(key);
            return new JsonArrayBuilder(this);
        }

        public JsonObjectBuilder object(String key) throws IOException {
            writeKey(key);
            return new JsonObjectBuilder(this);
        }

        public JsonObjectBuilder build() throws IOException {
            writer.append('}');
            return parent;
        }

        //------------------------------------------< private >---

        private void optionalComma() throws IOException {
            if (hasKeys) {
                writer.append(',');
            } else {
                hasKeys = true;
            }
        }

        private void writeKey(String key) throws IOException {
            optionalComma();
            writer.append(quote(escape(key)));
            writer.append(':');
        }

        private void write(String key, String value) throws IOException {
            writeKey(key);
            writer.append(value);
        }

    }

    public final class JsonArrayBuilder {
        private final JsonObjectBuilder parent;

        private boolean hasValues;

        public JsonArrayBuilder(JsonObjectBuilder parent) throws IOException {
            writer.append('[');
            this.parent = parent;
        }

        public JsonArrayBuilder value(String value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(int value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(long value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(float value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(double value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(Number value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder value(boolean value) throws IOException {
            optionalComma();
            writer.append(encode(value));
            return this;
        }

        public JsonArrayBuilder nil() throws IOException {
            optionalComma();
            writer.append("null");
            return this;
        }

        public JsonObjectBuilder build() throws IOException {
            writer.append(']');
            return parent;
        }

        //------------------------------------------< private >---

        private void optionalComma() throws IOException {
            if (hasValues) {
                writer.append(',');
            } else {
                hasValues = true;
            }
        }
    }

    /**
     * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters (U+0000 through U+001F).
     */
    public static String escape(String string) {
        if (string == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
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
                    } else {
                        sb.append(ch);
                    }
            }
        }

        return sb.toString();
    }

    public static String quote(String string) {
        return '"' + string + '"';
    }

    public static String encode(String value) {
        return quote(escape(value));
    }

    public static String encode(int value) {
        return Integer.toString(value);
    }

    public static String encode(long value) {
        return Long.toString(value);
    }

    public static String encode(float value) {
        // TODO silently losing data, should probably throw an exception instead
        return Float.isInfinite(value) || Float.isNaN(value)
                ? "null"
                : Float.toString(value);
    }

    public static String encode(double value) {
        // TODO silently losing data, should probably throw an exception instead
        return Double.isInfinite(value) || Double.isNaN(value)
                ? "null"
                : Double.toString(value);
    }

    public static String encode(Number value) {
        return value.toString();
    }

    public static String encode(boolean value) {
        return Boolean.toString(value);
    }

    public static String encode(String[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (String value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(int[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(long[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (long value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(float[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (float value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(double[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (double value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(Number[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Number value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

    public static String encode(boolean[] values) {
        if (values.length == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (boolean value : values) {
            sb.append(encode(value));
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        return sb.toString();
    }

}
