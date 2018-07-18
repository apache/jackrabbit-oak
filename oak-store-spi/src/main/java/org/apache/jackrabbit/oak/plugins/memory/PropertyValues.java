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
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.collect.Iterables.contains;

/**
 * Utility class for creating {@link PropertyValue} instances.
 */
public final class PropertyValues {

    private PropertyValues() {
    }

    @Nullable
    public static PropertyValue create(@Nullable PropertyState property) {
        if (property == null) {
            return null;
        }
        return newValue(property);
    }

    @NotNull
    private static PropertyValue newValue(@NotNull PropertyState property) {
        return new PropertyStateValue(property);
    }

    @Nullable
    public static PropertyState create(@Nullable PropertyValue value) {
        if (value == null) {
            return null;
        }
        if (value instanceof PropertyStateValue) {
            return ((PropertyStateValue) value).unwrap();
        }
        return null;
    }

    @NotNull
    public static PropertyValue newString(@NotNull String value) {
        return new PropertyStateValue(StringPropertyState.stringProperty("", value));
    }

    @NotNull
    public static PropertyValue newString(@NotNull Iterable<String> value) {
        return new PropertyStateValue(MultiStringPropertyState.stringProperty("", value));
    }

    @NotNull
    public static PropertyValue newLong(@NotNull Long value) {
        return new PropertyStateValue(LongPropertyState.createLongProperty("", value));
    }

    @NotNull
    public static PropertyValue newDouble(@NotNull Double value) {
        return new PropertyStateValue(DoublePropertyState.doubleProperty("", value));
    }

    @NotNull
    public static PropertyValue newDecimal(@NotNull BigDecimal value) {
        return new PropertyStateValue(DecimalPropertyState.decimalProperty("", value));
    }

    @NotNull
    public static PropertyValue newBoolean(boolean value) {
        return new PropertyStateValue(BooleanPropertyState.booleanProperty("", value));
    }

    @NotNull
    public static PropertyValue newDate(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.dateProperty("", value));
    }

    @NotNull
    public static PropertyValue newName(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.nameProperty("", value));
    }

    @NotNull
    public static PropertyValue newName(@NotNull Iterable<String> value) {
        return new PropertyStateValue(MultiGenericPropertyState.nameProperty("", value));
    }

    @NotNull
    public static PropertyValue newPath(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.pathProperty("", value));
    }

    @NotNull
    public static PropertyValue newReference(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.referenceProperty("", value));
    }

    @NotNull
    public static PropertyValue newWeakReference(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.weakreferenceProperty("", value));
    }

    @NotNull
    public static PropertyValue newUri(@NotNull String value) {
        return new PropertyStateValue(GenericPropertyState.uriProperty("", value));
    }

    @NotNull
    public static PropertyValue newBinary(@NotNull byte[] value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
    }
    
    @NotNull
    public static PropertyValue newBinary(@NotNull Blob value) {
        return new PropertyStateValue(BinaryPropertyState.binaryProperty("", value));
    }

    // --

    public static boolean match(@NotNull PropertyValue p1, @NotNull PropertyState p2) {
        return match(p1, newValue(p2));
    }

    public static boolean match(@NotNull PropertyState p1, @NotNull PropertyValue p2) {
        return match(newValue(p1), p2);
    }

    public static boolean match(@NotNull PropertyValue p1, @NotNull PropertyValue p2) {
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

    public static boolean notMatch(@NotNull PropertyValue p1, @NotNull PropertyValue p2) {
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
