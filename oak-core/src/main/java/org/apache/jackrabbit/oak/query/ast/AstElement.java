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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.Query;

/**
 * The base class for all abstract syntax tree nodes.
 */
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

    protected String quote(String pathOrName) {
        return '[' + pathOrName + ']';
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

    protected Tree getTree(String path) {
        return query.getTree(path);
    }

}

