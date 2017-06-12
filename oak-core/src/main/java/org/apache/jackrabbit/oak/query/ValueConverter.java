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

import java.net.URI;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueConverter {

    private static final Logger log = LoggerFactory.getLogger(ValueConverter.class);

    private ValueConverter() {}

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

    /**
     * Converts the given value to a value of the specified target type. The
     * conversion is performed according to the rules described in
     * "3.6.4 Property Type Conversion" in the JCR 2.0 specification.
     *
     * @param value the value to convert
     * @param targetType the target property type
     * @param mapper the name mapper or {@code null} if no name/path mapping is required.
     * @return the converted value
     * @throws IllegalArgumentException if mapping is illegal
     */
    public static PropertyValue convert(@Nonnull PropertyValue value, int targetType, @Nullable NamePathMapper mapper) {
        int sourceType = value.getType().tag();
        if (sourceType == targetType) {
            return value;
        }
        switch (targetType) {
            case PropertyType.BINARY:
                Blob blob = value.getValue(Type.BINARY);
                return PropertyValues.newBinary(blob);
            case PropertyType.BOOLEAN:
                return PropertyValues.newBoolean(value.getValue(Type.BOOLEAN));
            case PropertyType.DATE:
                return PropertyValues.newDate(value.getValue(Type.DATE));
            case PropertyType.DOUBLE:
                return PropertyValues.newDouble(value.getValue(Type.DOUBLE));
            case PropertyType.LONG:
                return PropertyValues.newLong(value.getValue(Type.LONG));
            case PropertyType.DECIMAL:
                return PropertyValues.newDecimal(value.getValue(Type.DECIMAL));
        }
        // for other types, the value is first converted to a string
        String v = value.getValue(Type.STRING);
        switch (targetType) {
            case PropertyType.STRING:
                return PropertyValues.newString(v);
            case PropertyType.PATH:
                switch (sourceType) {
                    case PropertyType.BINARY:
                    case PropertyType.STRING:
                    case PropertyType.NAME:
                        return PropertyValues.newPath(v);
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
                        return PropertyValues.newPath(v);
                }
                break;
            case PropertyType.NAME:
                switch (sourceType) {
                    case PropertyType.BINARY:
                    case PropertyType.STRING:
                    case PropertyType.PATH:
                        // path might be a name (relative path of length 1)
                        // try conversion via string
                        return PropertyValues.newName(getOakPath(v, mapper));
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
                        return PropertyValues.newName(getOakPath(v, mapper));
                }
                break;
            case PropertyType.REFERENCE:
                switch (sourceType) {
                    case PropertyType.BINARY:
                    case PropertyType.STRING:
                    case PropertyType.WEAKREFERENCE:
                        return PropertyValues.newReference(v);
                }
                break;
            case PropertyType.WEAKREFERENCE:
                switch (sourceType) {
                    case PropertyType.BINARY:
                    case PropertyType.STRING:
                    case PropertyType.REFERENCE:
                        return PropertyValues.newWeakReference(v);
                }
                break;
            case PropertyType.URI:
                switch (sourceType) {
                    case PropertyType.BINARY:
                    case PropertyType.STRING:
                        return PropertyValues.newUri(v);
                    case PropertyType.NAME:
                        // prefix name with "./" (JCR 2.0 spec 3.6.4.8)
                        return PropertyValues.newUri("./" + v);
                    case PropertyType.PATH:
                        // prefix name with "./" (JCR 2.0 spec 3.6.4.9)
                        return PropertyValues.newUri("./" + v);
                }
        }
        throw new IllegalArgumentException(
                "Unsupported conversion from property type " +
                        PropertyType.nameFromValue(sourceType) +
                        " to property type " +
                        PropertyType.nameFromValue(targetType));
    }

    private static String getOakPath(@Nonnull String jcrPath, @CheckForNull NamePathMapper mapper) {
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