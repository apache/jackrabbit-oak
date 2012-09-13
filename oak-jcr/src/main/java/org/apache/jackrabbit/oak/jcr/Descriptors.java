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

package org.apache.jackrabbit.oak.jcr;

import java.util.HashMap;
import java.util.Map;

import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import static javax.jcr.Repository.IDENTIFIER_STABILITY;
import static javax.jcr.Repository.LEVEL_1_SUPPORTED;
import static javax.jcr.Repository.LEVEL_2_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_AUTOCREATED_DEFINITIONS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_INHERITANCE;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_INHERITANCE_SINGLE;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_MULTIPLE_BINARY_PROPERTIES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_MULTIVALUED_PROPERTIES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_ORDERABLE_CHILD_NODES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_OVERRIDES_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_PRIMARY_ITEM_NAME_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_PROPERTY_TYPES;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_RESIDUAL_DEFINITIONS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_SAME_NAME_SIBLINGS_SUPPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_UPDATE_IN_USE_SUPORTED;
import static javax.jcr.Repository.NODE_TYPE_MANAGEMENT_VALUE_CONSTRAINTS_SUPPORTED;
import static javax.jcr.Repository.OPTION_ACCESS_CONTROL_SUPPORTED;
import static javax.jcr.Repository.OPTION_ACTIVITIES_SUPPORTED;
import static javax.jcr.Repository.OPTION_BASELINES_SUPPORTED;
import static javax.jcr.Repository.OPTION_JOURNALED_OBSERVATION_SUPPORTED;
import static javax.jcr.Repository.OPTION_LIFECYCLE_SUPPORTED;
import static javax.jcr.Repository.OPTION_LOCKING_SUPPORTED;
import static javax.jcr.Repository.OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED;
import static javax.jcr.Repository.OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_OBSERVATION_SUPPORTED;
import static javax.jcr.Repository.OPTION_QUERY_SQL_SUPPORTED;
import static javax.jcr.Repository.OPTION_RETENTION_SUPPORTED;
import static javax.jcr.Repository.OPTION_SHAREABLE_NODES_SUPPORTED;
import static javax.jcr.Repository.OPTION_SIMPLE_VERSIONING_SUPPORTED;
import static javax.jcr.Repository.OPTION_TRANSACTIONS_SUPPORTED;
import static javax.jcr.Repository.OPTION_UNFILED_CONTENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED;
import static javax.jcr.Repository.OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED;
import static javax.jcr.Repository.OPTION_VERSIONING_SUPPORTED;
import static javax.jcr.Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_EXPORT_SUPPORTED;
import static javax.jcr.Repository.OPTION_XML_IMPORT_SUPPORTED;
import static javax.jcr.Repository.QUERY_FULL_TEXT_SEARCH_SUPPORTED;
import static javax.jcr.Repository.QUERY_JOINS;
import static javax.jcr.Repository.QUERY_JOINS_NONE;
import static javax.jcr.Repository.QUERY_LANGUAGES;
import static javax.jcr.Repository.QUERY_STORED_QUERIES_SUPPORTED;
import static javax.jcr.Repository.QUERY_XPATH_DOC_ORDER;
import static javax.jcr.Repository.QUERY_XPATH_POS_INDEX;
import static javax.jcr.Repository.REP_NAME_DESC;
import static javax.jcr.Repository.REP_VENDOR_DESC;
import static javax.jcr.Repository.REP_VENDOR_URL_DESC;
import static javax.jcr.Repository.SPEC_NAME_DESC;
import static javax.jcr.Repository.SPEC_VERSION_DESC;
import static javax.jcr.Repository.WRITE_SUPPORTED;

public class Descriptors {

    private final Map<String, Descriptor> descriptors;

    @SuppressWarnings("deprecation")
    public Descriptors(ValueFactory valueFactory) {
        descriptors = new HashMap<String, Descriptor>();
        Value trueValue = valueFactory.createValue(true);
        Value falseValue = valueFactory.createValue(false);

        put(new Descriptor(
                IDENTIFIER_STABILITY,
                valueFactory.createValue(Repository.IDENTIFIER_STABILITY_METHOD_DURATION), true, true));
        put(new Descriptor(
                LEVEL_1_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                LEVEL_2_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_AUTOCREATED_DEFINITIONS_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_INHERITANCE,
                valueFactory.createValue(NODE_TYPE_MANAGEMENT_INHERITANCE_SINGLE), true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_MULTIPLE_BINARY_PROPERTIES_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_MULTIVALUED_PROPERTIES_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_ORDERABLE_CHILD_NODES_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_OVERRIDES_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_PRIMARY_ITEM_NAME_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_PROPERTY_TYPES,
                new Value[] {
                    valueFactory.createValue(PropertyType.TYPENAME_STRING),
                    valueFactory.createValue(PropertyType.TYPENAME_BINARY),
                    valueFactory.createValue(PropertyType.TYPENAME_LONG),
                    valueFactory.createValue(PropertyType.TYPENAME_LONG),
                    valueFactory.createValue(PropertyType.TYPENAME_DOUBLE),
                    valueFactory.createValue(PropertyType.TYPENAME_DECIMAL),
                    valueFactory.createValue(PropertyType.TYPENAME_DATE),
                    valueFactory.createValue(PropertyType.TYPENAME_BOOLEAN),
                    valueFactory.createValue(PropertyType.TYPENAME_NAME),
                    valueFactory.createValue(PropertyType.TYPENAME_PATH),
                    valueFactory.createValue(PropertyType.TYPENAME_REFERENCE),
                    valueFactory.createValue(PropertyType.TYPENAME_WEAKREFERENCE),
                    valueFactory.createValue(PropertyType.TYPENAME_URI),
                    valueFactory.createValue(PropertyType.TYPENAME_UNDEFINED)
                }, false, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_RESIDUAL_DEFINITIONS_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_SAME_NAME_SIBLINGS_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_VALUE_CONSTRAINTS_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                NODE_TYPE_MANAGEMENT_UPDATE_IN_USE_SUPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_ACCESS_CONTROL_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_JOURNALED_OBSERVATION_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_LIFECYCLE_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_LOCKING_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_OBSERVATION_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_QUERY_SQL_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_RETENTION_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_SHAREABLE_NODES_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_SIMPLE_VERSIONING_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_TRANSACTIONS_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_UNFILED_CONTENT_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_VERSIONING_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_WORKSPACE_MANAGEMENT_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_XML_EXPORT_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_XML_IMPORT_SUPPORTED,
                trueValue, true, true));
        put(new Descriptor(
                OPTION_ACTIVITIES_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                OPTION_BASELINES_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                QUERY_FULL_TEXT_SEARCH_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                QUERY_JOINS,
                valueFactory.createValue(QUERY_JOINS_NONE), true, true));
        put(new Descriptor(
                QUERY_LANGUAGES,
                new Value[0], false, true));
        put(new Descriptor(
                QUERY_STORED_QUERIES_SUPPORTED,
                falseValue, true, true));
        put(new Descriptor(
                QUERY_XPATH_DOC_ORDER,
                falseValue, true, true));
        put(new Descriptor(
                QUERY_XPATH_POS_INDEX,
                falseValue, true, true));
        put(new Descriptor(
                REP_NAME_DESC,
                valueFactory.createValue("Apache Jackrabbit Oak JCR implementation"), true, true));
        put(new Descriptor(
                REP_VENDOR_DESC,
                valueFactory.createValue("Apache Software Foundation"), true, true));
        put(new Descriptor(
                REP_VENDOR_URL_DESC,
                valueFactory.createValue("http://www.apache.org/"), true, true));
        put(new Descriptor(
                SPEC_NAME_DESC,
                valueFactory.createValue("Content Repository for Java Technology API"), true, true));
        put(new Descriptor(
                SPEC_VERSION_DESC,
                valueFactory.createValue("2.0"), true, true));
        put(new Descriptor(
                WRITE_SUPPORTED,
                trueValue, true, true));
    }

    public Descriptors(ValueFactory valueFactory, Iterable<Descriptor> descriptors) {
        this(valueFactory);

        for (Descriptor d : descriptors) {
            this.descriptors.put(d.name, d);
        }
    }

    public String[] getKeys() {
        return descriptors.keySet().toArray(new String[descriptors.size()]);
    }

    public boolean isStandardDescriptor(String key) {
        return descriptors.containsKey(key) && descriptors.get(key).standard;
    }

    public boolean isSingleValueDescriptor(String key) {
        return descriptors.containsKey(key) && descriptors.get(key).singleValued;
    }

    public Value getValue(String key) {
        Descriptor d = descriptors.get(key);
        return d == null || !d.singleValued ? null : d.values[0];
    }

    public Value[] getValues(String key) {
        Descriptor d = descriptors.get(key);
        return d == null ? null : d.values;
    }

    public static final class Descriptor {
        final String name;
        final Value[] values;
        final boolean singleValued;
        final boolean standard;

        public Descriptor(String name, Value[] values, boolean singleValued, boolean standard) {
            this.name = name;
            this.values = values;
            this.singleValued = singleValued;
            this.standard = standard;
        }

        public Descriptor(String name, Value value, boolean singleValued, boolean standard) {
            this(name, new Value[]{ value }, singleValued, standard);
        }
    }

    //------------------------------------------< private >---

    private void put(Descriptor descriptor) {
        descriptors.put(descriptor.name, descriptor);
    }

}
