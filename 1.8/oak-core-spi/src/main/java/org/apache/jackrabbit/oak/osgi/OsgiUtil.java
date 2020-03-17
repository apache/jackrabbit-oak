/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.osgi;

import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.component.ComponentContext;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility methods to use in an OSGi environment.
 */
public class OsgiUtil {

    private OsgiUtil() {
        // Prevent instantiation.
    }

    /**
     * Looks a property up by name in a component context. Returns {@code null}
     * if the property is not found or if the property is found but it is an
     * empty string.
     *
     * @param context Component context.
     * @param name    Name of the property.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookup(ComponentContext context, String name) {
        return asString(checkNotNull(context).getProperties().get(checkNotNull(name)));
    }

    /**
     * Looks a property up by name in the set of framework properties. Returns
     * {@code null} if the property is not found or if the property is found but
     * it is an empty string.
     *
     * @param context Bundle context.
     * @param name    Name of the property.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookup(BundleContext context, String name) {
        return asString(checkNotNull(context).getProperty(checkNotNull(name)));
    }

    /**
     * Looks a property up by name in the component context first, falling back
     * in the framework properties if not found. Returns {@code null} if the
     * property is not found or if the property is found but it is an empty
     * string.
     *
     * @param context Component context.
     * @param name    Name of the property.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookupConfigurationThenFramework(ComponentContext context, String name) {
        return lookupConfigurationThenFramework(context, name, name);
    }

    /**
     * Looks a property up by name in the component context first, falling back
     * in the framework properties if not found. Returns {@code null} if the
     * property is not found or if the property is found but it is an empty
     * string.
     *
     * @param context         Component context.
     * @param nameInComponent Name of the property in the component context.
     * @param nameInFramework Name of the property in the framework properties.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookupConfigurationThenFramework(ComponentContext context, String nameInComponent, String nameInFramework) {
        String fromComponent = lookup(context, nameInComponent);

        if (fromComponent != null) {
            return fromComponent;
        }

        String fromFramework = lookup(context.getBundleContext(), nameInFramework);

        if (fromFramework != null) {
            return fromFramework;
        }

        return null;
    }

    /**
     * Looks a property up by name in the framework properties first, falling
     * back to the component context if not not found. Returns {@code null} if
     * the property is not found or if the property is found but it is an empty
     * string.
     *
     * @param context Component context.
     * @param name    Name of the property.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookupFrameworkThenConfiguration(ComponentContext context, String name) {
        return lookupFrameworkThenConfiguration(context, name, name);
    }

    /**
     * Looks a property up by name in the framework properties first, falling
     * back to the component context if not not found. Returns {@code null} if
     * the property is not found or if the property is found but it is an empty
     * string.
     *
     * @param context         Component context.
     * @param nameInComponent Name of the property in the component context.
     * @param nameInFramework Name of the property in the framework properties.
     * @return The property value serialized as a string, or {@code null}.
     */
    public static String lookupFrameworkThenConfiguration(ComponentContext context, String nameInComponent, String nameInFramework) {
        String fromFramework = lookup(checkNotNull(context).getBundleContext(), nameInFramework);

        if (fromFramework != null) {
            return fromFramework;
        }

        String fromComponent = lookup(context, nameInComponent);

        if (fromComponent != null) {
            return fromComponent;
        }

        return null;
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }

        String string = value.toString().trim();

        if (string.isEmpty()) {
            return null;
        }

        return string;
    }

    /**
     * Create a {@link Filter} using the passed Class as an objectClass and the map
     * as the filter attributes.
     * @param clazz the target objectClass
     * @param attributes target attributes (null value for the absence)
     * @return OSGi filter representing the input
     */
    public static Filter getFilter(Class<?> clazz, Map<String, String> attributes) {
        StringBuilder filterBuilder = new StringBuilder("(&");
        appendLdapFilterAttribute(filterBuilder, Constants.OBJECTCLASS, clazz.getName());
        for (Map.Entry<String, String> e : attributes.entrySet()) {
            appendLdapFilterAttribute(filterBuilder, e.getKey(), e.getValue());
        }
        filterBuilder.append(')');
        try {
            return FrameworkUtil.createFilter(filterBuilder.toString());
        } catch(InvalidSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static StringBuilder appendLdapFilterAttribute(StringBuilder filterBuilder, String key, String value) {
        if (value == null) {
            filterBuilder.append("(!(").append(key).append("=*))");
        } else {
            filterBuilder.append("(").append(key).append("=");
            appendEscapedLdapValue(filterBuilder, value);
            filterBuilder.append(")");
        }
        return filterBuilder;
    }

    static StringBuilder appendEscapedLdapValue(StringBuilder filterBuilder, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c == '\\') || (c == '(') || (c == ')') || (c == '*')) {
                filterBuilder.append('\\');
            }
            filterBuilder.append(c);
        }
        return filterBuilder;
    }
}
