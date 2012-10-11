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
package org.apache.jackrabbit.oak.value;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

public class PropertyValues {

    private PropertyValues() {
    }

    // TODO consistent naming

    @CheckForNull
    public static PropertyValue create(PropertyState property) {
        if (property == null) {
            return null;
        }
        return new PropertyValue(property);
    }

    @Nonnull
    public static PropertyValue newString(String value) {
        return new PropertyValue(PropertyStates.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newString(Iterable<String> value) {
        return new PropertyValue(PropertyStates.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newLong(Long value) {
        return new PropertyValue(PropertyStates.longProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDouble(Double value) {
        return new PropertyValue(PropertyStates.doubleProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDecimal(BigDecimal value) {
        return new PropertyValue(PropertyStates.decimalProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBoolean(boolean value) {
        return new PropertyValue(PropertyStates.booleanProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDate(String value) {
        return new PropertyValue(PropertyStates.dateProperty("", value));
    }

    @Nonnull
    public static PropertyValue newName(String value) {
        return new PropertyValue(PropertyStates.nameProperty("", value));
    }

    @Nonnull
    public static PropertyValue newPath(String value) {
        return new PropertyValue(PropertyStates.pathProperty("", value));
    }

    @Nonnull
    public static PropertyValue newReference(String value) {
        return new PropertyValue(PropertyStates.referenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newWeakReference(String value) {
        return new PropertyValue(
                PropertyStates.weakreferenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newUri(String value) {
        return new PropertyValue(PropertyStates.uriProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBinary(byte[] value) {
        return new PropertyValue(PropertyStates.binaryProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBinary(Blob value) {
        return new PropertyValue(PropertyStates.binaryProperty("", value));
    }

    // --

    public static boolean match(PropertyValue p1, PropertyState p2) {
        return match(p1, create(p2));
    }

    public static boolean match(PropertyState p1, PropertyValue p2) {
        return match(create(p1), p2);
    }

    public static boolean match(PropertyValue p1, PropertyValue p2) {
        if (p1.getType().tag() != p2.getType().tag()) {
            return false;
        }

        switch (p1.getType().tag()) {
        case PropertyType.BINARY:
            if (p1.isArray() && !p2.isArray()) {
                return contains(p1.getValue(Type.BINARIES),
                        p2.getValue(Type.BINARY));
            }
            if (!p1.isArray() && p2.isArray()) {
                return contains(p2.getValue(Type.BINARIES),
                        p2.getValue(Type.BINARY));
            }
        default:
            if (p1.isArray() && !p2.isArray()) {
                return contains(p1.getValue(Type.STRINGS),
                        p2.getValue(Type.STRING));
            }
            if (!p1.isArray() && p2.isArray()) {
                return contains(p2.getValue(Type.STRINGS),
                        p2.getValue(Type.STRING));
            }
        }
        // both arrays or both single values
        return p1.compareTo(p2) == 0;

    }

    private static <T extends Comparable<T>> boolean contains(Iterable<T> p1,
            T p2) {
        Iterator<T> i1 = p1.iterator();
        while (i1.hasNext()) {
            int compare = i1.next().compareTo(p2);
            if (compare == 0) {
                return true;
            }
        }
        return false;
    }

    // --
    /**
     * Convert a value to the given target type, if possible.
     * 
     * @param value
     *            the value to convert
     * @param targetType
     *            the target property type
     * @return the converted value, or null if converting is not possible
     */
    public static PropertyValue convert(PropertyValue value, int targetType,
            NamePathMapper mapper) {
        // TODO support full set of conversion features defined in the JCR spec
        // at 3.6.4 Property Type Conversion
        // re-use existing code if possible
        try {
            switch (targetType) {
            case PropertyType.STRING:
                return newString(value.getValue(Type.STRING));
            case PropertyType.DATE:
                return newDate(value.getValue(Type.STRING));
            case PropertyType.LONG:
                return newLong(value.getValue(Type.LONG));
            case PropertyType.DOUBLE:
                return newDouble(value.getValue(Type.DOUBLE));
            case PropertyType.DECIMAL:
                return newDecimal(value.getValue(Type.DECIMAL));
            case PropertyType.BOOLEAN:
                return newBoolean(value.getValue(Type.BOOLEAN));
            case PropertyType.NAME:
                return newName(getOakPath(value.getValue(Type.STRING), mapper));
            case PropertyType.PATH:
                return newPath(value.getValue(Type.STRING));
            case PropertyType.REFERENCE:
                return newReference(value.getValue(Type.STRING));
            case PropertyType.WEAKREFERENCE:
                return newWeakReference(value.getValue(Type.STRING));
            case PropertyType.URI:
                return newUri(value.getValue(Type.STRING));
            case PropertyType.BINARY:
                try {
                    byte[] data = value.getValue(Type.STRING).getBytes("UTF-8");
                    return newBinary(data);
                } catch (IOException e) {
                    // I don't know in what case that could really occur
                    // except if UTF-8 isn't supported
                    throw new IllegalArgumentException(
                            value.getValue(Type.STRING), e);
                }
            }
            return null;
            // throw new IllegalArgumentException("Unknown property type: " +
            // targetType);
        } catch (UnsupportedOperationException e) {
            // TODO detect unsupported conversions, so that no exception is
            // thrown
            // because exceptions are slow
            return null;
            // throw new IllegalArgumentException("<unsupported conversion of "
            // +
            // v + " (" + PropertyType.nameFromValue(v.getType()) + ") to type "
            // +
            // PropertyType.nameFromValue(targetType) + ">");
        }
    }

    public static String getOakPath(String jcrPath, NamePathMapper mapper) {
        if (mapper == null) {
            // to simplify testing, a getNamePathMapper isn't required
            return jcrPath;
        }
        String p = mapper.getOakPath(jcrPath);
        if (p == null) {
            throw new IllegalArgumentException("Not a valid JCR path: "
                    + jcrPath);
        }
        return p;
    }

}
