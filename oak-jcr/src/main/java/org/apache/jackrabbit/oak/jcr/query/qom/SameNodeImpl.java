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

import javax.jcr.query.qom.SameNode;

/**
 * The implementation of the corresponding JCR interface.
 */
public class SameNodeImpl extends ConstraintImpl implements SameNode {

    private final String path;
    private final String selectorName;

    public SameNodeImpl(String selectorName, String path) {
        this.selectorName = selectorName;
        this.path = path;
    }

    @Override
    public String getSelectorName() {
        return selectorName;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("ISSAMENODE(");
        if (selectorName != null) {
            buff.append(quoteSelectorName(selectorName)).append(", ");
        }
        buff.append(quotePath(path)).append(')');
        return buff.toString();
    }

    @Override
    public void bindVariables(QueryObjectModelImpl qom) {
        // ignore
    }

}
