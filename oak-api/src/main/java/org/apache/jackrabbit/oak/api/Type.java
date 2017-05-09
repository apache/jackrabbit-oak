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
package org.apache.jackrabbit.oak.api;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

/**
 * Instances of this class map Java types to {@link PropertyType property types}.
 * Passing an instance of this class to {@link PropertyState#getValue(Type)} determines
 * the return type of that method.
 * @param <T>
 */
public final class Type<T> implements Comparable<Type<?>> {

    private static final Map<String, Type<?>> TYPES = new HashMap<String, Type<?>>();

    private static <T> Type<T> create(int tag, boolean array, String string) {
        Type<T> type = new Type<T>(tag, array, string);
        TYPES.put(string, type);
        return type;
    }

    /** Map {@code String} to {@link PropertyType#STRING} */
    public static final Type<String> STRING =
            create(PropertyType.STRING, false, "STRING");

    /** Map {@code Blob} to {@link PropertyType#BINARY} */
    public static final Type<Blob> BINARY =
            create(PropertyType.BINARY, false, "BINARY");

    /** Map {@code Long} to {@link PropertyType#LONG} */
    public static final Type<Long> LONG =
            create(PropertyType.LONG, false, "LONG");

    /** Map {@code Double} to {@link PropertyType#DOUBLE} */
    public static final Type<Double> DOUBLE =
            create(PropertyType.DOUBLE, false, "DOUBLE");

    /** Map {@code String} to {@link PropertyType#DATE} */
    public static final Type<String> DATE =
            create(PropertyType.DATE, false, "DATE");

    /** Map {@code Boolean} to {@link PropertyType#BOOLEAN} */
    public static final Type<Boolean> BOOLEAN =
            create(PropertyType.BOOLEAN, false, "BOOLEAN");

    /** Map {@code String} to {@link PropertyType#STRING} */
    public static final Type<String> NAME =
            create(PropertyType.NAME, false, "NAME");

    /** Map {@code String} to {@link PropertyType#PATH} */
    public static final Type<String> PATH =
            create(PropertyType.PATH, false, "PATH");

    /** Map {@code String} to {@link PropertyType#REFERENCE} */
    public static final Type<String> REFERENCE =
            create(PropertyType.REFERENCE, false, "REFERENCE");

    /** Map {@code String} to {@link PropertyType#WEAKREFERENCE} */
    public static final Type<String> WEAKREFERENCE =
            create(PropertyType.WEAKREFERENCE, false, "WEAKREFERENCE");

    /** Map {@code String} to {@link PropertyType#URI} */
    public static final Type<String> URI =
            create(PropertyType.URI, false, "URI");

    /** Map {@code BigDecimal} to {@link PropertyType#DECIMAL} */
    public static final Type<BigDecimal> DECIMAL =
            create(PropertyType.DECIMAL, false, "DECIMAL");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#STRING} */
    public static final Type<Iterable<String>> STRINGS =
            create(PropertyType.STRING, true, "STRINGS");

    /** Map {@code Iterable<Blob>} to array of {@link PropertyType#BINARY} */
    public static final Type<Iterable<Blob>> BINARIES =
            create(PropertyType.BINARY, true, "BINARIES");

    /** Map {@code Iterable<Long>} to array of {@link PropertyType#LONG} */
    public static final Type<Iterable<Long>> LONGS =
            create(PropertyType.LONG, true, "LONGS");

    /** Map {@code Iterable<Double>} to array of {@link PropertyType#DOUBLE} */
    public static final Type<Iterable<Double>> DOUBLES =
            create(PropertyType.DOUBLE, true, "DOUBLES");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#DATE} */
    public static final Type<Iterable<String>> DATES =
            create(PropertyType.DATE, true, "DATES");

    /** Map {@code Iterable<Boolean>} to array of {@link PropertyType#BOOLEAN} */
    public static final Type<Iterable<Boolean>> BOOLEANS =
            create(PropertyType.BOOLEAN, true, "BOOLEANS");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#NAME} */
    public static final Type<Iterable<String>> NAMES =
            create(PropertyType.NAME, true, "NAMES");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#PATH} */
    public static final Type<Iterable<String>> PATHS =
            create(PropertyType.PATH, true, "PATHS");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#REFERENCE} */
    public static final Type<Iterable<String>> REFERENCES =
            create(PropertyType.REFERENCE, true, "REFERENCES");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#WEAKREFERENCE} */
    public static final Type<Iterable<String>> WEAKREFERENCES =
            create(PropertyType.WEAKREFERENCE, true, "WEAKREFERENCES");

    /** Map {@code Iterable<String>} to array of {@link PropertyType#URI} */
    public static final Type<Iterable<String>> URIS =
            create(PropertyType.URI, true, "URIS");

    /** Map {@code Iterable<BigDecimal>} to array of {@link PropertyType#DECIMAL} */
    public static final Type<Iterable<BigDecimal>> DECIMALS =
            create(PropertyType.DECIMAL, true, "DECIMALS");

    /** The special "undefined" type, never encountered in normal values */
    public static final Type<Void> UNDEFINED =
            create(PropertyType.UNDEFINED, false, "UNDEFINED");

    /** Multi-valued "undefined" type, never encountered in normal values */
    public static final Type<Iterable<Void>> UNDEFINEDS =
            create(PropertyType.UNDEFINED, true, "UNDEFINEDS");

    private final int tag;

    private final boolean array;

    private final String string;

    private Type(int tag, boolean array, String string) {
        this.tag = tag;
        this.array = array;
        this.string = string;
    }

    /**
     * Corresponding type tag as defined in {@link PropertyType}.
     * @return  type tag
     */
    public int tag() {
        return tag;
    }

    /**
     * Determine whether this is an array type
     * @return  {@code true} if and only if this is an array type
     */
    public boolean isArray() {
        return array;
    }

    /**
     * Corresponding {@code Type} for a given type tag and array flag.
     * @param tag  type tag as defined in {@link PropertyType}.
     * @param array  whether this is an array or not
     * @return  {@code Type} instance
     * @throws IllegalArgumentException if tag is not valid as per definition in {@link PropertyType}.
     */
    public static Type<?> fromTag(int tag, boolean array) {
        switch (tag) {
            case PropertyType.STRING: return array ? STRINGS : STRING;
            case PropertyType.BINARY: return array ? BINARIES : BINARY;
            case PropertyType.LONG: return array ? LONGS : LONG;
            case PropertyType.DOUBLE: return array ? DOUBLES : DOUBLE;
            case PropertyType.DATE: return array ? DATES: DATE;
            case PropertyType.BOOLEAN: return array ? BOOLEANS: BOOLEAN;
            case PropertyType.NAME: return array ? NAMES : NAME;
            case PropertyType.PATH: return array ? PATHS: PATH;
            case PropertyType.REFERENCE: return array ? REFERENCES : REFERENCE;
            case PropertyType.WEAKREFERENCE: return array ? WEAKREFERENCES : WEAKREFERENCE;
            case PropertyType.URI: return array ? URIS: URI;
            case PropertyType.DECIMAL: return array ? DECIMALS : DECIMAL;
            case PropertyType.UNDEFINED: return array ? UNDEFINEDS : UNDEFINED;
            default: throw new IllegalArgumentException("Invalid type tag: " + tag);
        }
    }

    /**
     * Returns the {@code Type} with the given string representation.
     *
     * @param string type string
     * @return matching type
     */
    public static Type<?> fromString(String string) {
        Type<?> type = TYPES.get(string);
        if (type == null) {
            throw new IllegalArgumentException("Invalid type name: " + string);
        }
        return type;
    }

    /**
     * Determine the base type of array types
     * @return  base type
     * @throws IllegalStateException if {@code isArray} is false.
     */
    public Type<?> getBaseType() {
        checkState(isArray(), "Not an array");
        return fromTag(tag, false);
    }

    /**
     * Determine the array type which has this type as base type
     * @return  array type with this type as base type
     * @throws IllegalStateException if {@code isArray} is true.
     */
    public Type<?> getArrayType() {
        checkState(!isArray(), "Not a simply type");
        return fromTag(tag, true);
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(@Nonnull Type<?> that) {
        if (tag < that.tag) {
            return -1;
        }

        if (tag > that.tag) {
            return 1;
        }

        if (!array && that.array) {
            return -1;
        }

        if (array && !that.array) {
            return 1;
        }

        return 0;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return string;
    }

    @Override
    public int hashCode() {
        int result = 1;

        result = result + 31 * tag;
        result = result + 31 * hashCode(array);

        return result;
    }

    private int hashCode(boolean value) {
        return value ? 1231 : 1237;
    }

    private void checkState(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

}
