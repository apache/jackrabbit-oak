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

import javax.jcr.PropertyType;

import com.google.common.base.Objects;

/**
 * Instances of this class map Java types to {@link PropertyType property types}.
 * Passing an instance of this class to {@link PropertyState#getValue(Type)} determines
 * the return type of that method.
 * @param <T>
 */
public final class Type<T> {

    /** Map {@code String} to {@link PropertyType#STRING} */
    public static final Type<String> STRING = create(PropertyType.STRING, false);

    /** Map {@code Blob} to {@link PropertyType#BINARY} */
    public static final Type<Blob> BINARY = create(PropertyType.BINARY, false);

    /** Map {@code Long} to {@link PropertyType#LONG} */
    public static final Type<Long> LONG = create(PropertyType.LONG, false);

    /** Map {@code Double} to {@link PropertyType#DOUBLE} */
    public static final Type<Double> DOUBLE = create(PropertyType.DOUBLE, false);

    /** Map {@code String} to {@link PropertyType#DATE} */
    public static final Type<String> DATE = create(PropertyType.DATE, false);

    /** Map {@code Boolean} to {@link PropertyType#BOOLEAN} */
    public static final Type<Boolean> BOOLEAN = create(PropertyType.BOOLEAN, false);

    /** Map {@code String} to {@link PropertyType#STRING} */
    public static final Type<String> NAME = create(PropertyType.NAME, false);

    /** Map {@code String} to {@link PropertyType#PATH} */
    public static final Type<String> PATH = create(PropertyType.PATH, false);

    /** Map {@code String} to {@link PropertyType#REFERENCE} */
    public static final Type<String> REFERENCE = create(PropertyType.REFERENCE, false);

    /** Map {@code String} to {@link PropertyType#WEAKREFERENCE} */
    public static final Type<String> WEAKREFERENCE = create(PropertyType.WEAKREFERENCE, false);

    /** Map {@code String} to {@link PropertyType#URI} */
    public static final Type<String> URI = create(PropertyType.URI, false);

    /** Map {@code BigDecimal} to {@link PropertyType#DECIMAL} */
    public static final Type<BigDecimal> DECIMAL = create(PropertyType.DECIMAL, false);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#STRING} */
    public static final Type<Iterable<String>> STRINGS = create(PropertyType.STRING, true);

    /** Map {@code Iterable<Blob} to array of {@link PropertyType#BINARY} */
    public static final Type<Iterable<Blob>> BINARIES = create(PropertyType.BINARY, true);

    /** Map {@code Iterable<Long>} to array of {@link PropertyType#LONG} */
    public static final Type<Iterable<Long>> LONGS = create(PropertyType.LONG, true);

    /** Map {@code Iterable<Double>} to array of {@link PropertyType#DOUBLE} */
    public static final Type<Iterable<Double>> DOUBLES = create(PropertyType.DOUBLE, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#DATE} */
    public static final Type<Iterable<String>> DATES = create(PropertyType.DATE, true);

    /** Map {@code Iterable<Boolean>} to array of {@link PropertyType#BOOLEAN} */
    public static final Type<Iterable<Boolean>> BOOLEANS = create(PropertyType.BOOLEAN, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#NAME} */
    public static final Type<Iterable<String>> NAMES = create(PropertyType.NAME, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#PATH} */
    public static final Type<Iterable<String>> PATHS = create(PropertyType.PATH, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#REFERENCE} */
    public static final Type<Iterable<String>> REFERENCES = create(PropertyType.REFERENCE, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#WEAKREFERENCE} */
    public static final Type<Iterable<String>> WEAKREFERENCES = create(PropertyType.WEAKREFERENCE, true);

    /** Map {@code Iterable<String>} to array of {@link PropertyType#URI} */
    public static final Type<Iterable<String>> URIS = create(PropertyType.URI, true);

    /** Map {@code Iterable<BigDecimal>} to array of {@link PropertyType#DECIMAL} */
    public static final Type<Iterable<BigDecimal>> DECIMALS = create(PropertyType.DECIMAL, true);

    private final int tag;
    private final boolean array;

    private Type(int tag, boolean array){
        this.tag = tag;
        this.array = array;
    }

    private static <T> Type<T> create(int tag, boolean array) {
        return new Type<T>(tag, array);
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
            default: throw new IllegalArgumentException("Invalid type tag: " + tag);
        }
    }

    /**
     * Determine the base type of array types
     * @return  base type
     * @throws IllegalStateException if {@code isArray} is false.
     */
    public Type<?> getBaseType() {
        if (!isArray()) {
            throw new IllegalStateException("Not an array: " + this);
        }
        return fromTag(tag, false);
    }

    @Override
    public String toString() {
        return isArray()
            ? "[]" + PropertyType.nameFromValue(getBaseType().tag)
            : PropertyType.nameFromValue(tag);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tag, array);
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

}
