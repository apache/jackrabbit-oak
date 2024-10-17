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

package org.apache.jackrabbit.oak.plugins.index.search.util;

import java.util.Collections;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.primitives.Ints;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * Utility class to retrieve configuration values for index definitions.
 */
public class ConfigUtil {

    private static final String ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE = "Multiple values provided for property %s in index definition . Single value was expected";

    public static boolean getOptionalValue(NodeState definition, String propName, boolean defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : ps.getValue(Type.BOOLEAN);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static int getOptionalValue(NodeState definition, String propName, int defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : Ints.checkedCast(ps.getValue(Type.LONG));
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static String getOptionalValue(NodeState definition, String propName, String defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : ps.getValue(Type.STRING);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static float getOptionalValue(NodeState definition, String propName, float defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : ps.getValue(Type.DOUBLE).floatValue();
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static double getOptionalValue(NodeState definition, String propName, double defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : ps.getValue(Type.DOUBLE);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static long getOptionalValue(NodeState definition, String propName, long defaultVal) {
        try {
            PropertyState ps = definition.getProperty(propName);
            return ps == null ? defaultVal : ps.getValue(Type.LONG);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, propName), e);
        }
    }

    public static String getPrimaryTypeName(NodeState nodeState) {
        try {
            PropertyState ps = nodeState.getProperty(JcrConstants.JCR_PRIMARYTYPE);
            return (ps == null) ? JcrConstants.NT_BASE : ps.getValue(Type.NAME);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(String.format(ILLEGAL_STATE_EXCEPTION_ERROR_MESSAGE, JcrConstants.JCR_PRIMARYTYPE), e);
        }
    }

    public static Iterable<String> getMixinNames(NodeState nodeState) {
        PropertyState ps = nodeState.getProperty(JcrConstants.JCR_MIXINTYPES);
        return (ps == null) ? Collections.emptyList() : ps.getValue(Type.NAMES);
    }

    /**
     * Assumes that given state is of type nt:file and then reads
     * the jcr:content/@jcr:data property to get the binary content
     */
    @Nullable
    public static Blob getBlob(NodeState state, String resourceName){
        NodeState contentNode = state.getChildNode(JcrConstants.JCR_CONTENT);
        checkArgument(contentNode.exists(), "Was expecting to find jcr:content node to read resource %s", resourceName);
        PropertyState property = contentNode.getProperty(JcrConstants.JCR_DATA);
        return property != null ? property.getValue(Type.BINARY) : null;
    }

    /**
     * Returns an array of optional values for the a given property
     */
    public static<T> T[] getOptionalValues(NodeState definition, String propName, Type<Iterable<T>> type, Class<T> typeParam) {
        return getOptionalValues(definition, propName, type, typeParam, null);
    }

    /**
     * Returns an array of optional values for the a given property if present, otherwise returns the specified default
     */
    public static<T> T[] getOptionalValues(NodeState definition, String propName, Type<Iterable<T>> type, Class<T> typeParam, T[] defaultValues) {
        PropertyState ps = definition.getProperty(propName);
        if (ps != null) {
            return Iterables.toArray(ps.getValue(type), typeParam);
        }
        return defaultValues;
    }
}
