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

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.kernel.CoreValueMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.Query;

import javax.jcr.PropertyType;

abstract class AstElement {

    protected Query query;

    abstract boolean accept(AstVisitor v);

    protected String protect(Object expression) {
        String str = expression.toString();
        if (str.indexOf(' ') >= 0) {
            return '(' + str + ')';
        } else {
            return str;
        }
    }

    protected String quotePath(String path) {
        return '[' + path + ']';
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    /**
     * Calculate the absolute path (the path including the workspace name).
     *
     * @param path the session local path
     * @return the absolute path
     */
    protected String getAbsolutePath(String path) {
        return path;
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
     * Convert the JSON property value to a core value.
     *
     * @param propertyValue JSON property value
     * @return the core value
     */
    protected CoreValue getCoreValue(String propertyValue) {
        // TODO data type mapping
        CoreValueFactory vf = query.getValueFactory();
        JsopReader r = new JsopTokenizer(propertyValue);
        if (r.matches('[')) {
            // TODO support arrays, but only for comparisons
            throw new IllegalArgumentException("Arrays are currently not supported: " + propertyValue);
        }
        return CoreValueMapper.fromJsopReader(r, vf);
    }

    /**
     * Validate that the given value can be converted to a JCR name.
     *
     * @param v the value
     * @return true if it can be converted
     */
    protected boolean isName(CoreValue v) {
        // TODO correctly validate JCR names - see JCR 2.0 spec 3.2.4 Naming Restrictions
        switch (v.getType()) {
        case PropertyType.DATE:
        case PropertyType.DECIMAL:
        case PropertyType.DOUBLE:
        case PropertyType.LONG:
        case PropertyType.BOOLEAN:
            return false;
        }
        String n = v.getString();
        if (n.startsWith("[") && !n.endsWith("]")) {
            return false;
        }
        return true;
    }

    protected String getOakPath(String jcrPath) {
        NamePathMapper m = query.getNamePathMapper();
        if (m == null) {
            // to simplify testing, a getNamePathMapper isn't required
            return jcrPath;
        }
        String p = m.getOakPath(jcrPath);
        if (p == null) {
            throw new IllegalArgumentException("Not a valid JCR path: " + jcrPath);
        }
        return p;
    }

    protected Tree getTree(String path) {
        return query.getTree(path);
    }

}

