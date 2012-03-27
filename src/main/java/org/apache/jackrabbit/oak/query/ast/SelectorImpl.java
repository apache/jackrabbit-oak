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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.CoreValue;
import org.apache.jackrabbit.oak.query.index.Cursor;
import org.apache.jackrabbit.oak.query.index.Filter;
import org.apache.jackrabbit.oak.query.index.NodeReader;
import org.apache.jackrabbit.oak.query.index.TraversingReader;

public class SelectorImpl extends SourceImpl {

    // TODO jcr:path isn't an official feature, support it?
    private static final String PATH = "jcr:path";

    protected NodeReader reader;

    private final String nodeTypeName, selectorName;
    private Cursor cursor;

    public SelectorImpl(String nodeTypeName, String selectorName) {
        this.nodeTypeName = nodeTypeName;
        this.selectorName = selectorName;
    }

    public String getNodeTypeName() {
        return nodeTypeName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        // TODO quote nodeTypeName?
        return nodeTypeName + " AS " + getSelectorName();
    }


    @Override
    public void prepare(MicroKernel mk) {
        reader = new TraversingReader(mk);
    }

    @Override
    public void execute(String revisionId) {
        cursor = reader.query(createFilter(), revisionId);
    }

    @Override
    public String getPlan() {
        return  nodeTypeName + " AS " + getSelectorName() + " /* " + reader.getPlan(createFilter()) + " */";
    }

    private Filter createFilter() {
        Filter f = new Filter(this);
        if (joinCondition != null) {
            joinCondition.apply(f);
        }
        if (!outerJoin) {
            // for outer joins, query constraints can't be applied to the
            // filter, because that would alter the result
            if (queryConstraint != null) {
                queryConstraint.apply(f);
            }
        }
        return f;
    }

    @Override
    public boolean next() {
        return cursor == null ? false : cursor.next();
    }

    @Override
    public String currentPath() {
        return cursor == null ? null : cursor.currentPath();
    }

    @Override
    public NodeImpl currentNode() {
        return cursor == null ? null : cursor.currentNode();
    }

    public CoreValue currentProperty(String propertyName) {
        if (propertyName.equals(PATH)) {
            String p = currentPath();
            return p == null ? null : query.getValueFactory().createValue(p);
        }
        NodeImpl n = currentNode();
        if (n == null) {
            return null;
        }
        String value = n.getProperty(propertyName);
        if (value == null) {
            return null;
        }
        // TODO data type mapping
        value = JsopTokenizer.decodeQuoted(value);
        return query.getValueFactory().createValue(value);
    }

    @Override
    public void init(Query qom) {
        // nothing to do
    }

    @Override
    public SelectorImpl getSelector(String selectorName) {
        if (selectorName.equals(this.selectorName)) {
            return this;
        }
        return null;
    }

}
