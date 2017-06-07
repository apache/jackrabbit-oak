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
package org.apache.jackrabbit.oak.plugins.memory;

import java.math.BigDecimal;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;

import static com.google.common.collect.Iterables.contains;

/**
 * Utility class for creating {@link PropertyValue} instances.
 */
public final class PropertyValues {

    private PropertyValues() {
    }

    @CheckForNull
    public static PropertyValue create(@CheckForNull PropertyState property) {
        if (property == null) {
            return null;
        }
        return newValue(property);
    }

    @Nonnull
    private static PropertyValue newValue(@Nonnull PropertyState property) {
        return new PropertyStateValue(property);
    }

    @CheckForNull
    public static PropertyState create(@CheckForNull PropertyValue value) {
        if (value == null) {
            return null;
        }
        if (value instanceof PropertyStateValue) {
            return ((PropertyStateValue) value).unwrap();
        }
        return null;
    }

    @Nonnull
    public static PropertyValue newString(@Nonnull String value) {
        return new PropertyStateValue(StringPropertyState.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newString(@Nonnull Iterable<String> value) {
        return new PropertyStateValue(MultiStringPropertyState.stringProperty("", value));
    }

    @Nonnull
    public static PropertyValue newLong(@Nonnull Long value) {
        return new PropertyStateValue(LongPropertyState.createLongProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDouble(@Nonnull Double value) {
        return new PropertyStateValue(DoublePropertyState.doubleProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDecimal(@Nonnull BigDecimal value) {
        return new PropertyStateValue(DecimalPropertyState.decimalProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBoolean(boolean value) {
        return new PropertyStateValue(BooleanPropertyState.booleanProperty("", value));
    }

    @Nonnull
    public static PropertyValue newDate(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.dateProperty("", value));
    }

    @Nonnull
    public static PropertyValue newName(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.nameProperty("", value));
    }

    @Nonnull
    public static PropertyValue newName(@Nonnull Iterable<String> value) {
        return new PropertyStateValue(MultiGenericPropertyState.nameProperty("", value));
    }

    @Nonnull
    public static PropertyValue newPath(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.pathProperty("", value));
    }

    @Nonnull
    public static PropertyValue newReference(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.referenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newWeakReference(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.weakreferenceProperty("", value));
    }

    @Nonnull
    public static PropertyValue newUri(@Nonnull String value) {
        return new PropertyStateValue(GenericPropertyState.uriProperty("", value));
    }

    @Nonnull
    public static PropertyValue newBinary(@Nonnull byte[] value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
    }
    
    @Nonnull
    public static PropertyValue newBinary(@Nonnull Blob value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
    }

    // --

    public static boolean match(@Nonnull PropertyValue p1, @Nonnull PropertyState p2) {
        return match(p1, newValue(p2));
    }

    public static boolean match(@Nonnull PropertyState p1, @Nonnull PropertyValue p2) {
        return match(newValue(p1), p2);
    }

    public static boolean match(@Nonnull PropertyValue p1, @Nonnull PropertyValue p2) {
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
                        p1.getValue(Type.BINARY));
            }
            break;
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

    public static boolean notMatch(@Nonnull PropertyValue p1, @Nonnull PropertyValue p2) {
        if (p1.getType().tag() != p2.getType().tag()) {
            return true;
        }

        switch (p1.getType().tag()) {
        case PropertyType.BINARY:
            if (p1.isArray() && !p2.isArray()) {
                if (p1.count() > 1) {
                    // a value can not possibly match multiple distinct values
                    return true;
                }
                return !contains(p1.getValue(Type.BINARIES),
                        p2.getValue(Type.BINARY));
            }
            if (!p1.isArray() && p2.isArray()) {
                if (p2.count() > 1) {
                    // a value can not possibly match multiple distinct values
                    return true;
                }
                return !contains(p2.getValue(Type.BINARIES),
                        p1.getValue(Type.BINARY));
            }
            break;
        default:
            if (p1.isArray() && !p2.isArray()) {
                if (p1.count() > 1) {
                    // a value can not possibly match multiple distinct values
                    return true;
                }
                return !contains(p1.getValue(Type.STRINGS),
                        p2.getValue(Type.STRING));
            }
            if (!p1.isArray() && p2.isArray()) {
                if (p2.count() > 1) {
                    // a value can not possibly match multiple distinct values
                    return true;
                }
                return !contains(p2.getValue(Type.STRINGS),
                        p1.getValue(Type.STRING));
            }
        }
        // both arrays or both single values
        return p1.compareTo(p2) != 0;

    }

    // --

}
