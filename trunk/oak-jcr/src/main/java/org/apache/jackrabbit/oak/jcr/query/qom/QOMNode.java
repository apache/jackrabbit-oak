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
package org.apache.jackrabbit.oak.jcr.query.qom;

/**
 * The base class for all QOM nodes.
 */
abstract class QOMNode {
    
    protected String protect(Object expression) {
        String str = expression.toString();
        if (str.indexOf(' ') >= 0) {
            return '(' + str + ')';
        }
        return str;
    }

    protected String quotePath(String path) {
        return quoteName(path);
    }

    protected String quoteSelectorName(String name) {
        return quoteName(name);
    }

    protected String quotePropertyName(String name) {
        return quoteName(name);
    }

    protected String quoteColumnName(String name) {
        return quoteName(name);
    }

    protected String quoteNodeTypeName(String name) {
        return quoteName(name);
    }

    private static String quoteName(String name) {
        name = name.replaceAll("]", "]]");
        return '[' + name + ']';
    }

}

