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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.jcr.PropertyType.BINARY;
import static javax.jcr.PropertyType.BOOLEAN;
import static javax.jcr.PropertyType.DATE;
import static javax.jcr.PropertyType.DECIMAL;
import static javax.jcr.PropertyType.DOUBLE;
import static javax.jcr.PropertyType.LONG;
import static javax.jcr.PropertyType.NAME;
import static javax.jcr.PropertyType.PATH;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.STRING;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_BOOLEAN;
import static javax.jcr.PropertyType.TYPENAME_DATE;
import static javax.jcr.PropertyType.TYPENAME_DECIMAL;
import static javax.jcr.PropertyType.TYPENAME_DOUBLE;
import static javax.jcr.PropertyType.TYPENAME_LONG;
import static javax.jcr.PropertyType.TYPENAME_NAME;
import static javax.jcr.PropertyType.TYPENAME_PATH;
import static javax.jcr.PropertyType.TYPENAME_REFERENCE;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static javax.jcr.PropertyType.TYPENAME_UNDEFINED;
import static javax.jcr.PropertyType.TYPENAME_URI;
import static javax.jcr.PropertyType.TYPENAME_WEAKREFERENCE;
import static javax.jcr.PropertyType.UNDEFINED;
import static javax.jcr.PropertyType.URI;
import static javax.jcr.PropertyType.WEAKREFERENCE;

/**
 * <pre>
 * [nt:propertyDefinition]
 *   ...
 * - jcr:requiredType (STRING) protected mandatory
 *   < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
 *     'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
 *     'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
 * - jcr:valueConstraints (STRING) protected multiple
 * - jcr:defaultValues (UNDEFINED) protected multiple
 * - jcr:multiple (BOOLEAN) protected mandatory
 * - jcr:availableQueryOperators (NAME) protected mandatory multiple
 * - jcr:isFullTextSearchable (BOOLEAN) protected mandatory
 * - jcr:isQueryOrderable (BOOLEAN) protected mandatory
 * </pre>
 */
class PropertyDefinitionImpl extends ItemDefinitionImpl
        implements PropertyDefinition {

    private static final Logger log =
            LoggerFactory.getLogger(PropertyDefinitionImpl.class);

    private final ValueFactory factory;

    public PropertyDefinitionImpl(NodeType type, ValueFactory factory, NodeUtil node) {
        super(type, node);
        this.factory = factory;
    }

    /**
     * Returns the numeric constant value of the type with the specified name.
     *
     * In contrast to {@link javax.jcr.PropertyType#valueFromName(String)} this method
     * requires all type names to be all upper case.
     * See also: OAK-294 and http://java.net/jira/browse/JSR_283-811
     *
     * @param name the name of the property type.
     * @return the numeric constant value.
     * @throws IllegalArgumentException if {@code name} is not a valid property type name.
     */
    public static int valueFromName(String name) {
        if (name.equals(TYPENAME_STRING.toUpperCase())) {
            return STRING;
        } else if (name.equals(TYPENAME_BINARY.toUpperCase())) {
            return BINARY;
        } else if (name.equals(TYPENAME_BOOLEAN.toUpperCase())) {
            return BOOLEAN;
        } else if (name.equals(TYPENAME_LONG.toUpperCase())) {
            return LONG;
        } else if (name.equals(TYPENAME_DOUBLE.toUpperCase())) {
            return DOUBLE;
        } else if (name.equals(TYPENAME_DECIMAL.toUpperCase())) {
            return DECIMAL;
        } else if (name.equals(TYPENAME_DATE.toUpperCase())) {
            return DATE;
        } else if (name.equals(TYPENAME_NAME.toUpperCase())) {
            return NAME;
        } else if (name.equals(TYPENAME_PATH.toUpperCase())) {
            return PATH;
        } else if (name.equals(TYPENAME_REFERENCE.toUpperCase())) {
            return REFERENCE;
        } else if (name.equals(TYPENAME_WEAKREFERENCE.toUpperCase())) {
            return WEAKREFERENCE;
        } else if (name.equals(TYPENAME_URI.toUpperCase())) {
            return URI;
        } else if (name.equals(TYPENAME_UNDEFINED.toUpperCase())) {
            return UNDEFINED;
        } else {
            throw new IllegalArgumentException("unknown type: " + name);
        }
    }

    @Override
    public int getRequiredType() {
        try {
            return valueFromName(node.getString("jcr:requiredType", TYPENAME_UNDEFINED));
        } catch (IllegalArgumentException e) {
            log.warn("Unexpected jcr:requiredType value", e);
            return UNDEFINED;
        }
    }

    @Override
    public String[] getValueConstraints() {
        // TODO: namespace mapping?
        String[] constraints = node.getStrings("jcr:valueConstraints");
        if (constraints == null) {
            constraints = new String[0];
        }
        return constraints;
    }

    @Override
    public Value[] getDefaultValues() {
        if (factory != null) {
            return node.getValues("jcr:defaultValues", factory);
        }
        else {
            log.warn("Cannot create default values: no value factory");
            return null;
        }
    }

    @Override
    public boolean isMultiple() {
        return node.getBoolean("jcr:multiple");
    }

    @Override
    public String[] getAvailableQueryOperators() {
        String[] ops = node.getStrings("jcr:availableQueryOperators");
        if (ops == null) {
            ops = new String[] {
                    QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN,
                    QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN,
                    QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                    QueryObjectModelConstants.JCR_OPERATOR_LIKE };
        }
        return ops;
    }

    @Override
    public boolean isFullTextSearchable() {
        return node.getBoolean("jcr:isFullTextSearchable");
    }

    @Override
    public boolean isQueryOrderable() {
        return node.getBoolean("jcr:isQueryOrderable");
    }

}
