/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An {@link OakSolrConfiguration} specified via a given {@link org.apache.jackrabbit.oak.spi.state.NodeState}.
 * For each of the supported properties a default is provided if either the
 * property doesn't exist in the node or if the value is <code>null</code>
 */
public class OakSolrNodeStateConfiguration implements OakSolrConfiguration {

    private final NodeState definition;

    public OakSolrNodeStateConfiguration(NodeState definition) {
        this.definition = definition;
        if (!definition.hasProperty("type") || !(SolrQueryIndex.TYPE.equals(definition.getProperty("type").getValue(Type.STRING)))) {
            throw new IllegalArgumentException("missing or wrong 'type' property in " + definition);
        }
    }

    @Override
    public String getFieldNameFor(Type<?> propertyType) {
        Iterable<String> typeMappings = getStringValuesFor(Properties.TYPE_MAPPINGS);
        if (typeMappings != null) {
            for (String typeMapping : typeMappings) {
                String[] mapping = typeMapping.split("=");
                if (mapping.length == 2 && mapping[0] != null && mapping[1] != null) {
                    Type<?> type = Type.fromString(mapping[0]);
                    if (type != null && type.tag() == propertyType.tag()) {
                        return mapping[1];
                    }
                }
            }
        }
        return null;
    }

    @Nonnull
    @Override
    public String getPathField() {
        return getStringValueFor(Properties.PATH_FIELD, OakSolrConfigurationDefaults.PATH_FIELD_NAME);
    }

    @CheckForNull
    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = getStringValueFor(Properties.DESCENDANTS_FIELD, OakSolrConfigurationDefaults.DESC_FIELD_NAME);
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = getStringValueFor(Properties.CHILDREN_FIELD, OakSolrConfigurationDefaults.CHILD_FIELD_NAME);
                break;
            }
            case EXACT: {
                fieldName = getStringValueFor(Properties.PATH_FIELD, OakSolrConfigurationDefaults.PATH_FIELD_NAME);
                break;
            }
            case PARENT: {
                fieldName = getStringValueFor(Properties.PARENT_FIELD, OakSolrConfigurationDefaults.ANC_FIELD_NAME);
                break;
            }
            case NO_RESTRICTION:
                break;
            default:
                break;

        }
        return fieldName;
    }

    @Override
    public String getCatchAllField() {
        return getStringValueFor(Properties.CATCHALL_FIELD, OakSolrConfigurationDefaults.CATCHALL_FIELD);
    }

    @Override
    public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
        Iterable<String> propertyMappings = getStringValuesFor(Properties.PROPERTY_MAPPINGS);
        if (propertyMappings != null) {
            for (String propertyMapping : propertyMappings) {
                String[] mapping = propertyMapping.split("=");
                if (mapping.length == 2 && mapping[0] != null && mapping[1] != null) {
                    if (propertyRestriction.propertyName.equals(mapping[0])) {
                        return mapping[1];
                    }
                }
            }
        }
        return null;
    }

    @Nonnull
    @Override
    public CommitPolicy getCommitPolicy() {
        return CommitPolicy.valueOf(getStringValueFor(Properties.COMMIT_POLICY, CommitPolicy.SOFT.toString()).toUpperCase());
    }

    @Override
    public int getRows() {
        return getIntValueFor(Properties.ROWS, OakSolrConfigurationDefaults.ROWS);
    }

    @Override
    public boolean useForPropertyRestrictions() {
        return getBooleanValueFor(Properties.PROPERTY_RESTRICIONS, OakSolrConfigurationDefaults.PROPERTY_RESTRICTIONS);
    }

    @Override
    public boolean useForPrimaryTypes() {
        return getBooleanValueFor(Properties.PRIMARY_TYPES, OakSolrConfigurationDefaults.PRIMARY_TYPES);
    }

    @Override
    public boolean useForPathRestrictions() {
        return getBooleanValueFor(Properties.PATH_RESTRICTIONS, OakSolrConfigurationDefaults.PATH_RESTRICTIONS);
    }

    @Nonnull
    @Override
    public Collection<String> getIgnoredProperties() {
        Collection<String> ignoredProperties;
        Iterable<String> ignoredPropertiesValues = getStringValuesFor(Properties.IGNORED_PROPERTIES);
        if (ignoredPropertiesValues != null) {
            ignoredProperties = new LinkedList<String>();
            for (String ignoredProperty : ignoredPropertiesValues) {
                ignoredProperties.add(ignoredProperty);
            }
        } else {
            ignoredProperties = OakSolrConfigurationDefaults.IGNORED_PROPERTIES;
        }
        return ignoredProperties;
    }

    @Nonnull
    @Override
    public Collection<String> getUsedProperties() {
        Collection<String> usedProperties;
        Iterable<String> usedPropertiesValues = getStringValuesFor(Properties.USED_PROPERTIES);
        if (usedPropertiesValues != null) {
            usedProperties = new LinkedList<String>();
            for (String usedProperty : usedPropertiesValues) {
                usedProperties.add(usedProperty);
            }
        } else {
            usedProperties = Collections.emptyList();
        }
        return usedProperties;
    }

    @Override
    public boolean collapseJcrContentNodes() {
        return getBooleanValueFor(Properties.COLLAPSE_JCR_CONTENT_NODES, OakSolrConfigurationDefaults.COLLAPSE_JCR_CONTENT_NODES);
    }

    @Nonnull
    @Override
    public String getCollapsedPathField() {
        return getStringValueFor(Properties.COLLAPSED_PATH_FIELD, OakSolrConfigurationDefaults.COLLAPSED_PATH_FIELD);
    }

    @Nonnull
    @Override
    public String getPathDepthField() {
        return getStringValueFor(Properties.DEPTH_FIELD, OakSolrConfigurationDefaults.PATH_DEPTH_FIELD);
    }

    private boolean getBooleanValueFor(String propertyName, boolean defaultValue) {
        boolean value = defaultValue;
        PropertyState property = definition.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.BOOLEAN);
        }
        return value;
    }

    private int getIntValueFor(String propertyName, int defaultValue) {
        long value = defaultValue;
        PropertyState property = definition.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.LONG);
        }
        return (int) value;
    }

    private String getStringValueFor(String propertyName, String defaultValue) {
        String value = defaultValue;
        PropertyState property = definition.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.STRING);
        }
        return value;
    }

    private Iterable<String> getStringValuesFor(String propertyName) {
        Iterable<String> values = null;
        PropertyState property = definition.getProperty(propertyName);
        if (property != null && property.isArray()) {
            values = property.getValue(Type.STRINGS);
        }
        return values;
    }

    @Override
    public String toString() {
        return "OakSolrNodeStateConfiguration{" +
                "definitionChildren=" + Iterables.toString(definition.getChildNodeNames()) +
                '}';
    }

    /**
     * Properties that may be retrieved from the configuration {@link org.apache.jackrabbit.oak.spi.state.NodeState}.
     */
    public final class Properties {
        // --> oak solr config properties <--
        public static final String PATH_FIELD = "pathField";
        public static final String COLLAPSED_PATH_FIELD = "pathField";
        public static final String PARENT_FIELD = "parentField";
        public static final String CHILDREN_FIELD = "childrenField";
        public static final String DESCENDANTS_FIELD = "descendantsField";
        public static final String CATCHALL_FIELD = "catchAllField";
        public static final String COMMIT_POLICY = "commitPolicy";
        public static final String ROWS = "rows";
        public static final String PROPERTY_RESTRICIONS = "propertyRestrictions";
        public static final String PRIMARY_TYPES = "primaryTypes";
        public static final String PATH_RESTRICTIONS = "pathRestrictions";
        public static final String IGNORED_PROPERTIES = "ignoredProperties";
        public static final String TYPE_MAPPINGS = "typeMappings";
        public static final String PROPERTY_MAPPINGS = "propertyMappings";
        public static final String USED_PROPERTIES = "usedProperties";
        public static final String COLLAPSE_JCR_CONTENT_NODES = "collapseJcrContentNodes";
        public static final String DEPTH_FIELD= "depthField";
    }
}
