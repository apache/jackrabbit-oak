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
package org.apache.jackrabbit.oak.commons;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Objects.ToStringHelper;

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! THIS UTILITY CLASS IS A COPY FROM APACHE SLING !!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

/**
 * The {@code PropertiesUtil} is a utility class providing some
 * useful utility methods for converting property types.
 */
public final class PropertiesUtil {

    private static Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

    private PropertiesUtil() {}

    /**
     * Returns the boolean value of the parameter or the
     * {@code defaultValue} if the parameter is {@code null}.
     * If the parameter is not a {@code Boolean} it is converted
     * by calling {@code Boolean.valueOf} on the string value of the
     * object.
     *
     * @param propValue the property value or {@code null}
     * @param defaultValue the default boolean value
     */
    public static boolean toBoolean(Object propValue, boolean defaultValue) {
        propValue = toObject(propValue);
        if (propValue instanceof Boolean) {
            return (Boolean) propValue;
        } else if (propValue != null) {
            return Boolean.parseBoolean(String.valueOf(propValue));
        }

        return defaultValue;
    }

    /**
     * Returns the parameter as a string or the
     * {@code defaultValue} if the parameter is {@code null}.
     * @param propValue the property value or {@code null}
     * @param defaultValue the default string value
     */
    public static String toString(Object propValue, String defaultValue) {
        propValue = toObject(propValue);
        return (propValue != null) ? propValue.toString() : defaultValue;
    }

    /**
     * Returns the parameter as a long or the
     * {@code defaultValue} if the parameter is {@code null} or if
     * the parameter is not a {@code Long} and cannot be converted to
     * a {@code Long} from the parameter's string value.
     *
     * @param propValue the property value or {@code null}
     * @param defaultValue the default long value
     */
    public static long toLong(Object propValue, long defaultValue) {
        propValue = toObject(propValue);
        if (propValue instanceof Long) {
            return (Long) propValue;
        } else if (propValue != null) {
            try {
                return Long.parseLong(String.valueOf(propValue));
            } catch (NumberFormatException nfe) {
                // don't care, fall through to default value
            }
        }

        return defaultValue;
    }

    /**
     * Returns the parameter as an integer or the
     * {@code defaultValue} if the parameter is {@code null} or if
     * the parameter is not an {@code Integer} and cannot be converted to
     * an {@code Integer} from the parameter's string value.
     *
     * @param propValue the property value or {@code null}
     * @param defaultValue the default integer value
     */
    public static int toInteger(Object propValue, int defaultValue) {
        propValue = toObject(propValue);
        if (propValue instanceof Integer) {
            return (Integer) propValue;
        } else if (propValue != null) {
            try {
                return Integer.parseInt(String.valueOf(propValue));
            } catch (NumberFormatException nfe) {
                // don't care, fall through to default value
            }
        }

        return defaultValue;
    }

    /**
     * Returns the parameter as a double or the
     * {@code defaultValue} if the parameter is {@code null} or if
     * the parameter is not a {@code Double} and cannot be converted to
     * a {@code Double} from the parameter's string value.
     *
     * @param propValue the property value or {@code null}
     * @param defaultValue the default double value
     */
    public static double toDouble(Object propValue, double defaultValue) {
        propValue = toObject(propValue);
        if (propValue instanceof Double) {
            return (Double) propValue;
        } else if (propValue != null) {
            try {
                return Double.parseDouble(String.valueOf(propValue));
            } catch (NumberFormatException nfe) {
                // don't care, fall through to default value
            }
        }

        return defaultValue;
    }

    /**
     * Returns the parameter as a single value. If the
     * parameter is neither an array nor a {@code java.util.Collection} the
     * parameter is returned unmodified. If the parameter is a non-empty array,
     * the first array element is returned. If the property is a non-empty
     * {@code java.util.Collection}, the first collection element is returned.
     * Otherwise {@code null} is returned.
     *
     * @param propValue the parameter to convert.
     */
    public static Object toObject(Object propValue) {
        if (propValue == null) {
            return null;
        } else if (propValue.getClass().isArray()) {
            Object[] prop = (Object[]) propValue;
            return prop.length > 0 ? prop[0] : null;
        } else if (propValue instanceof Collection<?>) {
            Collection<?> prop = (Collection<?>) propValue;
            return prop.isEmpty() ? null : prop.iterator().next();
        } else {
            return propValue;
        }
    }

    /**
     * Returns the parameter as an array of Strings. If
     * the parameter is a scalar value its string value is returned as a single
     * element array. If the parameter is an array, the elements are converted to
     * String objects and returned as an array. If the parameter is a collection, the
     * collection elements are converted to String objects and returned as an array.
     * Otherwise (if the parameter is {@code null}) {@code null} is
     * returned.
     *
     * @param propValue The object to convert.
     */
    public static String[] toStringArray(Object propValue) {
        return toStringArray(propValue, null);
    }

    /**
     * Returns the parameter as an array of Strings. If
     * the parameter is a scalar value its string value is returned as a single
     * element array. If the parameter is an array, the elements are converted to
     * String objects and returned as an array. If the parameter is a collection, the
     * collection elements are converted to String objects and returned as an array.
     * Otherwise (if the property is {@code null}) a provided default value is
     * returned.
     *
     * @param propValue The object to convert.
     * @param defaultArray The default array to return.
     */
    public static String[] toStringArray(Object propValue, String[] defaultArray) {
        if (propValue == null) {
            // no value at all
            return defaultArray;

        } else if (propValue instanceof String) {
            // single string
            return new String[] { (String) propValue };

        } else if (propValue instanceof String[]) {
            // String[]
            return (String[]) propValue;

        } else if (propValue.getClass().isArray()) {
            // other array
            Object[] valueArray = (Object[]) propValue;
            List<String> values = new ArrayList<String>(valueArray.length);
            for (Object value : valueArray) {
                if (value != null) {
                    values.add(value.toString());
                }
            }
            return values.toArray(new String[values.size()]);

        } else if (propValue instanceof Collection<?>) {
            // collection
            Collection<?> valueCollection = (Collection<?>) propValue;
            List<String> valueList = new ArrayList<String>(valueCollection.size());
            for (Object value : valueCollection) {
                if (value != null) {
                    valueList.add(value.toString());
                }
            }
            return valueList.toArray(new String[valueList.size()]);
        }

        return defaultArray;
    }

    /**
     * Populates the bean properties from config instance. It supports coercing
     *  values for simple types like Number, Integer, Long, Boolean etc. Complex
     *  objects are not supported
     *
     * @param instance bean to populate
     * @param config properties to set in the passed bean
     * @param validate Flag to validate the configured bean property names against
     *                 the configured bean class
     */
    @SuppressWarnings("unchecked")
    public static void populate(Object instance, Map<String,?> config, boolean validate){
        Class<?> objectClass = instance.getClass();

        // Set all configured bean properties
        Map<String, Method> setters = getSetters(objectClass);
        ToStringHelper toStringHelper = Objects.toStringHelper(instance);
        for(Map.Entry<String,?> e : config.entrySet()) {
            String name = e.getKey();
            Method setter = setters.get(name);
            if (setter != null) {
                if (setter.getAnnotation(Deprecated.class) != null) {
                    log.warn("Parameter {} of {} has been deprecated",
                            name, objectClass.getName());
                }
                Object value = e.getValue();
                setProperty(instance, name, setter, value);
                toStringHelper.add(name,value);
            } else if (validate) {
                throw new IllegalArgumentException(
                        "Configured class " + objectClass.getName()
                                + " does not contain a property named " + name);
            }
        }

        log.debug("Configured object with properties {}", toStringHelper);
    }

    private static Map<String, Method> getSetters(Class<?> klass) {
        Map<String, Method> methods = new HashMap<String, Method>();
        for (Method method : klass.getMethods()) {
            String name = method.getName();
            if (name.startsWith("set") && name.length() > 3
                    && Modifier.isPublic(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())
                    && Void.TYPE.equals(method.getReturnType())
                    && method.getParameterTypes().length == 1) {
                methods.put(
                        name.substring(3, 4).toLowerCase(Locale.ENGLISH) + name.substring(4),
                        method);
            }
        }
        return methods;
    }

    private static void setProperty(
            Object instance, String name, Method setter, Object value) {
        String className = instance.getClass().getName();
        Class<?> type = setter.getParameterTypes()[0];
        try {
            if (type.isAssignableFrom(String.class)
                    || type.isAssignableFrom(Object.class)) {
                setter.invoke(instance, value);
            } else if (type.isAssignableFrom(Boolean.TYPE)
                    || type.isAssignableFrom(Boolean.class)) {
                setter.invoke(instance, toBoolean(value, false));
            } else if (type.isAssignableFrom(Integer.TYPE)
                    || type.isAssignableFrom(Integer.class)) {
                setter.invoke(instance, toInteger(value,0));
            } else if (type.isAssignableFrom(Long.TYPE)
                    || type.isAssignableFrom(Long.class)) {
                setter.invoke(instance, toLong(value, 0));
            } else if (type.isAssignableFrom(Double.TYPE)
                    || type.isAssignableFrom(Double.class)) {
                setter.invoke(instance, toDouble(value,0));
            } else {
                throw new RuntimeException(
                        "The type (" + type.getName()
                                + ") of property " + name + " of class "
                                + className + " is not supported");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    "Invalid number format (" + value + ") for property "
                            + name + " of class " + className, e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(
                    "Property " + name + " of class "
                            + className + " can not be set to \"" + value + '"',
                    e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "The setter of property " + name
                            + " of class " + className + " can not be accessed",
                    e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(
                    "Unable to call the setter of property "
                            + name + " of class " + className, e);
        }
    }
}
