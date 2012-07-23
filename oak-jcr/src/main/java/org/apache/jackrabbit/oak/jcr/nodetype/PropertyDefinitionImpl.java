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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.base.Joiner;

/**
 * Adapter class for turning an in-content property definition
 * node ("nt:propertyDefinition") to a {@link PropertyDefinition} instance.
 */
class PropertyDefinitionImpl extends ItemDefinitionImpl
        implements PropertyDefinition {

    public PropertyDefinitionImpl(NodeType type, Node node) {
        super(type, node);
    }

    //------------------------------------------------< PropertyDefinition >--

    /**
     * CND:
     * <pre>
     * - jcr:requiredType (STRING) protected mandatory
     *   < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
     *     'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
     *     'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
     * </pre>
     */
    @Override
    public int getRequiredType() {
        try {
            return PropertyType.valueFromName(
                    getString(Property.JCR_REQUIRED_TYPE));
        } catch (IllegalArgumentException e) {
            throw illegalState(e);
        }
    }

    /** CND: <pre>- jcr:valueConstraints (STRING) protected multiple</pre> */
    @Override
    public String[] getValueConstraints() {
        return getStrings(Property.JCR_VALUE_CONSTRAINTS, null);
    }

    /** CND: <pre>- jcr:defaultValues (UNDEFINED) protected multiple</pre> */
    @Override
    public Value[] getDefaultValues() {
        return getValues(Property.JCR_DEFAULT_VALUES, null);
    }

    /** CND: <pre>- jcr:multiple (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isMultiple() {
        return getBoolean(Property.JCR_MULTIPLE);
    }

    /** CND: <pre>- jcr:availableQueryOperators (NAME) protected mandatory multiple</pre> */
    @Override
    public String[] getAvailableQueryOperators() {
        return getStrings("jcr:availableQueryOperators", null); // TODO: constant
    }

    /** CND: <pre>- jcr:isFullTextSearchable (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isFullTextSearchable() {
        return getBoolean("jcr:isFullTextSearchable"); // TODO: constant
    }

    /** CND: <pre>- jcr:isQueryOrderable (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isQueryOrderable() {
        return getBoolean("jcr:isQueryOrderable"); // TODO: constant
    }

    //------------------------------------------------------------< Object >--

    public String toString() {
        String dv = null;
        String[] dvs = getStrings(Property.JCR_DEFAULT_VALUES, null);
        if (dvs != null) {
            dv = Joiner.on(", ").join(dvs);
        }

        StringBuilder sb = new StringBuilder("- ");
        appendItemCND(sb, PropertyType.nameFromValue(getRequiredType()), dv);
        sb.append(" ..."); // TODO: rest of the info
        return sb.toString();
    }

}
