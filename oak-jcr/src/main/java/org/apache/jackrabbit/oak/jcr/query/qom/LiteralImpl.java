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

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.qom.Literal;

/**
 * The implementation of the corresponding JCR interface.
 */
public class LiteralImpl extends StaticOperandImpl implements Literal {

    private final Value value;

    public LiteralImpl(Value value) {
        this.value = value;
    }

    @Override
    public Value getLiteralValue() {
        return value;
    }

    @Override
    public String toString() {
        try {
            switch (value.getType()) {
            case PropertyType.BINARY:
                return cast("BINARY");
            case PropertyType.BOOLEAN:
                return cast("BOOLEAN");
            case PropertyType.DATE:
                return cast("DATE");
            case PropertyType.DECIMAL:
                return cast("DECIMAL");
            case PropertyType.DOUBLE:
            case PropertyType.LONG:
                return value.getString();
            case PropertyType.NAME:
                return cast("NAME");
            case PropertyType.PATH:
                return cast("PATH");
            case PropertyType.REFERENCE:
                return cast("REFERENCE");
            case PropertyType.STRING:
                return escape(value.getString());
            case PropertyType.URI:
                return cast("URI");
            case PropertyType.WEAKREFERENCE:
                return cast("WEAKREFERENCE");
            default:
                return escape(value.getString());
            }
        } catch (RepositoryException e) {
            return value.toString();
        }
    }

    private String cast(String type) throws RepositoryException {
        return "CAST(" + escape(value.getString()) + " AS " + type + ')';
    }

    public static String escape(String v) {
        return '\'' + v.replace("'", "''") + '\'';
    }

    @Override
    public void bindVariables(QueryObjectModelImpl qom) {
        // ignore
    }

}
