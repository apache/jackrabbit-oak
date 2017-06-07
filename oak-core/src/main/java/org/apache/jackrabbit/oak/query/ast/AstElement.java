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
package org.apache.jackrabbit.oak.query.ast;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.ValueConverter;

/**
 * The base class for all abstract syntax tree nodes.
 */
abstract class AstElement {
    protected QueryImpl query;

    abstract boolean accept(AstVisitor v);

    protected String protect(Object expression) {
        String str = expression.toString();
        if (str.indexOf(' ') >= 0) {
            return '(' + str + ')';
        } else {
            return str;
        }
    }

    protected String quote(String pathOrName) {
        pathOrName = pathOrName.replaceAll("]", "]]");
        return '[' + pathOrName + ']';
    }

    protected String quoteJson(String string) {
        return '"' + string.replaceAll("\"", "\"\"") + '"';
    }

    public void setQuery(QueryImpl query) {
        this.query = query;
    }
    
    /**
     * Normalize the property name (including namespace remapping).
     * Asterisks are kept.
     *
     * @param propertyName the property name to normalize
     * @return the normalized (oak-) property name
     */
    protected String normalizePropertyName(String propertyName) {
        // TODO normalize the path (remove superfluous ".." and "." 
        // where possible)
        if (query == null) {
            return propertyName;
        }
        if (propertyName == null) {
            return null;
        }
        int slash = propertyName.indexOf('/');
        if (slash < 0) {
            return normalizeNonRelativePropertyName(propertyName);
        }
        // relative properties
        String relativePath = PathUtils.getParentPath(propertyName);
        if (relativePath.indexOf('*') >= 0) {
            StringBuilder buff = new StringBuilder();
            for (String p : PathUtils.elements(relativePath)) {
                if (!p.equals("*")) {
                    p = query.getOakPath(p);
                }
                if (p.length() > 0) {
                    if (buff.length() > 0) {
                        buff.append('/');
                    }
                    buff.append(p);
                }
            }
            relativePath = buff.toString();
        } else {
            relativePath = query.getOakPath(relativePath);
        }
        propertyName = PathUtils.getName(propertyName);
        propertyName = normalizeNonRelativePropertyName(propertyName);
        return PathUtils.concat(relativePath, propertyName);
    }
    
    private String normalizeNonRelativePropertyName(String propertyName) {
        if (propertyName.equals("*")) {
            return propertyName;
        }
        return query.getOakPath(propertyName);
    }

    /**
     * Validate and normalize the path.
     *
     * @param path the path to validate
     * @return the validated and normalized path
     */
    protected String normalizePath(String path) {
        // TODO normalize the path (remove superfluous ".." and "." 
        // where possible)
        if (query == null) {
            return path;
        }
        return query.getOakPath(path);
    }
    
    protected PropertyValue convertValueToType(PropertyValue v, PropertyValue targetType) {
        if (targetType.count() == 0) {
            return v;
        }
        int type = targetType.getType().tag();
        if (v.getType().tag() == type) {
            return v;
        }
        try {
            return ValueConverter.convert(v, type, query.getNamePathMapper());
        } catch (IllegalArgumentException e) {
            // not possible to convert
            return v;
        }
    }

    /**
     * Calculate the session local path (the path excluding the workspace name)
     * if possible.
     *
     * @param path the absolute path
     * @return the session local path, or null if not within this workspace
     */
    protected String getLocalPath(String path) {
        return path;
    }

    /**
     * @return a clone of self. Default implementation in {@link AstElement} returns same reference
     *         to {@code this}.
     */
    @Nonnull
    public AstElement copyOf() {
        return this;
    }    
}

