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
package org.apache.jackrabbit.oak.spi.query;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DecimalPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.GenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;

/**
 * Utility class for creating {@link PropertyValue} instances.
 */
public final class PropertyValues {

    private PropertyValues() {
    }

    @CheckForNull
    public static PropertyValue create(PropertyState property) {
        if (property == null) {
            return null;
        }
        return new PropertyStateValue(property);
    }

    @CheckForNull
    public static PropertyState create(PropertyValue value) {
        if (value == null) {
            return null;
        }
        if (value instanceof PropertyStateValue) {
            return ((PropertyStateValue) value).unwrap();
        }
        return null;
    }

    @Nonnull
    public static PropertyValue newString(String value) {
        return new PropertyStateValue(StringPropertyState.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newString(Iterable<String> value) {
        return new PropertyStateValue(MultiStringPropertyState.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newLong(Long value) {
        return new PropertyStateValue(LongPropertyState.createLongProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDouble(Double value) {
        return new PropertyStateValue(DoublePropertyState.doubleProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDecimal(BigDecimal value) {
        return new PropertyStateValue(DecimalPropertyState.decimalProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBoolean(boolean value) {
        return new PropertyStateValue(BooleanPropertyState.booleanProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDate(String value) {
        return new PropertyStateValue(LongPropertyState.createDateProperty("", value));
    }

    @Nonnull
    public static PropertyValue newName(String value) {
        return new PropertyStateValue(GenericPropertyState.nameProperty("", value));
    }

    @Nonnull
    public static PropertyValue newName(Iterable<String> value) {
        return new PropertyStateValue(MultiGenericPropertyState.nameProperty("", value));
    }

    @Nonnull
    public static PropertyValue newPath(String value) {
        return new PropertyStateValue(GenericPropertyState.pathProperty("", value));
    }

    @Nonnull
    public static PropertyValue newReference(String value) {
        return new PropertyStateValue(GenericPropertyState.referenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newWeakReference(String value) {
        return new PropertyStateValue(GenericPropertyState.weakreferenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newUri(String value) {
        return new PropertyStateValue(GenericPropertyState.uriProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBinary(byte[] value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
    }
    
    @Nonnull
    public static PropertyValue newBinary(Blob value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
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
                        p1.getValue(Type.STRING));
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
     * Converts the given value to a value of the specified target type. The
     * conversion is performed according to the rules described in
     * "3.6.4 Property Type Conversion" in the JCR 2.0 specification.
     * 
     * @param value the value to convert
     * @param targetType the target property type 
     * @param mapper the name mapper
     * @return the converted value
     * @throws IllegalArgumentException if mapping is illegal
     */
    public static PropertyValue convert(PropertyValue value, int targetType,
            NamePathMapper mapper) {
        int sourceType = value.getType().tag();
        if (sourceType == targetType) {
            return value;
        }
        switch (targetType) {
        case PropertyType.BINARY:
            Blob blob = value.getValue(Type.BINARY);
            return newBinary(blob);
        case PropertyType.BOOLEAN:
            return newBoolean(value.getValue(Type.BOOLEAN));
        case PropertyType.DATE:
            return newDate(value.getValue(Type.DATE));
        case PropertyType.DOUBLE:
            return newDouble(value.getValue(Type.DOUBLE));
        case PropertyType.LONG:
            return newLong(value.getValue(Type.LONG));
        case PropertyType.DECIMAL:
            return newDecimal(value.getValue(Type.DECIMAL));
        }
        // for other types, the value is first converted to a string
        String v = value.getValue(Type.STRING);
        switch (targetType) {
        case PropertyType.STRING:
            return newString(v);
        case PropertyType.PATH:
            switch (sourceType) {
            case PropertyType.BINARY:
            case PropertyType.STRING:
            case PropertyType.NAME:
                return newPath(v);
            case PropertyType.URI:
                URI uri = URI.create(v);
                if (uri.isAbsolute()) {
                    // uri contains scheme
                    throw new IllegalArgumentException(
                            "Failed to convert URI " + v + " to PATH");
                }
                String p = uri.getPath();
                if (p.startsWith("./")) {
                    p = p.substring(2);
                }
                return newPath(v);
            }
            break;
        case PropertyType.NAME: 
            switch (sourceType) {
            case PropertyType.BINARY:
            case PropertyType.STRING:
            case PropertyType.PATH:
                // path might be a name (relative path of length 1)
                // try conversion via string
                return newName(getOakPath(v, mapper));
            case PropertyType.URI:
                URI uri = URI.create(v);
                if (uri.isAbsolute()) {
                    // uri contains scheme
                    throw new IllegalArgumentException(
                            "Failed to convert URI " + v + " to PATH");
                }
                String p = uri.getPath();
                if (p.startsWith("./")) {
                    p = p.substring(2);
                }
                return newName(getOakPath(v, mapper));
            }
            break;
        case PropertyType.REFERENCE:
            switch (sourceType) {
            case PropertyType.BINARY:
            case PropertyType.STRING:
            case PropertyType.WEAKREFERENCE:
                return newReference(v);
            }
            break;
        case PropertyType.WEAKREFERENCE:
            switch (sourceType) {
            case PropertyType.BINARY:
            case PropertyType.STRING:
            case PropertyType.REFERENCE:
                return newWeakReference(v);
            }
            break;
        case PropertyType.URI:
            switch (sourceType) {
            case PropertyType.BINARY:
            case PropertyType.STRING:
                return newUri(v);
            case PropertyType.NAME:
                // prefix name with "./" (JCR 2.0 spec 3.6.4.8)
                return newUri("./" + v);
            case PropertyType.PATH:
                // prefix name with "./" (JCR 2.0 spec 3.6.4.9)
                return newUri("./" + v);
            }
        }
        throw new IllegalArgumentException(
                "Unsupported conversion from property type " + 
                        PropertyType.nameFromValue(sourceType) + 
                        " to property type " +
                        PropertyType.nameFromValue(targetType));
    }
    
    public static boolean canConvert(int sourceType, int targetType) {
        if (sourceType == targetType || 
                sourceType == PropertyType.UNDEFINED ||
                targetType == PropertyType.UNDEFINED) {
            return true;
        }
        switch (targetType) {
        case PropertyType.BINARY:
        case PropertyType.BOOLEAN:
        case PropertyType.DATE:
        case PropertyType.DOUBLE:
        case PropertyType.LONG:
        case PropertyType.DECIMAL:
        case PropertyType.STRING:
            return true;
        case PropertyType.NAME: 
        case PropertyType.PATH:
        case PropertyType.URI:
            switch (sourceType) {
            case PropertyType.NAME:
            case PropertyType.PATH:
            case PropertyType.URI:
                return true;
            }
            break;
        case PropertyType.REFERENCE:
        case PropertyType.WEAKREFERENCE:
            switch (sourceType) {
            case PropertyType.REFERENCE:
            case PropertyType.WEAKREFERENCE:
                return true;
            }
            break;
        }        
        if (sourceType == PropertyType.STRING || 
                sourceType == PropertyType.BINARY) {
            return true;
        }
        return false;
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
