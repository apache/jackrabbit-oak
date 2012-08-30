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
package org.apache.jackrabbit.oak.plugins.type;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Override
    public int getRequiredType() {
        try {
            return PropertyType.valueFromName(node.getString(
                    "jcr:requiredType", PropertyType.TYPENAME_UNDEFINED));
        } catch (IllegalArgumentException e) {
            log.warn("Unexpected jcr:requiredType value", e);
            return PropertyType.UNDEFINED;
        }
    }

    @Override
    public String[] getValueConstraints() {
        // TODO: namespace mapping?
        return node.getStrings("jcr:valueConstraints");
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
