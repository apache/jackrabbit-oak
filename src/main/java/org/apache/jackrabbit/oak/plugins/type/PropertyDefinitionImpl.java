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

import static javax.jcr.PropertyType.TYPENAME_UNDEFINED;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.query.qom.QueryObjectModelConstants;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;

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

    private static String[] DEFAULT_QOPS = new String[] {
        QueryObjectModelConstants.JCR_OPERATOR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_NOT_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN,
        QueryObjectModelConstants.JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN,
        QueryObjectModelConstants.JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO,
        QueryObjectModelConstants.JCR_OPERATOR_LIKE };

    public PropertyDefinitionImpl(NodeType type, NameMapper mapper, Tree tree) {
        super(type, mapper, tree);
    }

    @Override
    public int getRequiredType() {
        String type = getString("jcr:requiredType", TYPENAME_UNDEFINED);
        if (PropertyType.TYPENAME_BINARY.equalsIgnoreCase(type)) {
            return PropertyType.BINARY;
        } else if (PropertyType.TYPENAME_BOOLEAN.equalsIgnoreCase(type)) {
            return PropertyType.BOOLEAN;
        } else if (PropertyType.TYPENAME_DATE.equalsIgnoreCase(type)) {
            return PropertyType.DATE;
        } else if (PropertyType.TYPENAME_DECIMAL.equalsIgnoreCase(type)) {
            return PropertyType.DECIMAL;
        } else if (PropertyType.TYPENAME_DOUBLE.equalsIgnoreCase(type)) {
            return PropertyType.DOUBLE;
        } else if (PropertyType.TYPENAME_LONG.equalsIgnoreCase(type)) {
            return PropertyType.LONG;
        } else if (PropertyType.TYPENAME_NAME.equalsIgnoreCase(type)) {
            return PropertyType.NAME;
        } else if (PropertyType.TYPENAME_PATH.equalsIgnoreCase(type)) {
            return PropertyType.PATH;
        } else if (PropertyType.TYPENAME_REFERENCE.equalsIgnoreCase(type)) {
            return PropertyType.REFERENCE;
        } else if (PropertyType.TYPENAME_STRING.equalsIgnoreCase(type)) {
            return PropertyType.STRING;
        } else if (PropertyType.TYPENAME_URI.equalsIgnoreCase(type)) {
            return PropertyType.URI;
        } else if (PropertyType.TYPENAME_WEAKREFERENCE.equalsIgnoreCase(type)) {
            return PropertyType.WEAKREFERENCE;
        } else {
            return PropertyType.UNDEFINED;
        }
    }

    @Override
    public String[] getValueConstraints() {
        String[] constraints = getStrings("jcr:valueConstraints", null);
        if (constraints != null) {
            int type = getRequiredType();
            if (type == PropertyType.NAME || type == PropertyType.PATH) {
                for (int i = 0; i < constraints.length; i++) {
                    // TODO: namespace mapping
                }
            }
        }
        return constraints;
    }

    @Override
    public Value[] getDefaultValues() {
        return new Value[0]; // TODO
    }

    @Override
    public boolean isMultiple() {
        return getBoolean("jcr:multiple", false);
    }

    @Override
    public String[] getAvailableQueryOperators() {
        return getStrings("jcr:availableQueryOperators", DEFAULT_QOPS.clone());
    }

    @Override
    public boolean isFullTextSearchable() {
        return getBoolean("jcr:isFullTextSearchable", true);
    }

    @Override
    public boolean isQueryOrderable() {
        return getBoolean("jcr:isQueryOrderable", true);
    }

}
