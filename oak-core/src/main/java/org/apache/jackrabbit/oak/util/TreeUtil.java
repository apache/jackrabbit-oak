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
package org.apache.jackrabbit.oak.util;

import javax.annotation.CheckForNull;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * Utility providing common operations for the {@code Tree} that are not provided
 * by the API.
 */
public final class TreeUtil {

    private TreeUtil() {
    }

    @CheckForNull
    public static String getPrimaryTypeName(Tree tree) {
        return getString(tree, JcrConstants.JCR_PRIMARYTYPE);
    }

    @CheckForNull
    public static String[] getStrings(Tree tree, String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        if (property == null) {
            return null;
        } else {
            return Iterables.toArray(property.getValue(STRINGS), String.class);
        }
    }

    @CheckForNull
    public static String getString(Tree tree, String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        if (property != null && !property.isArray()) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }

    public static boolean getBoolean(Tree tree, String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        return property != null && !property.isArray() && property.getValue(BOOLEAN);
    }
}