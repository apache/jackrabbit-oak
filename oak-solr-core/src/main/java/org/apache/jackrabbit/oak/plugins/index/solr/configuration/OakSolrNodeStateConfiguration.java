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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.server.EmbeddedSolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An {@link OakSolrConfiguration} specified via a given {@link org.apache.jackrabbit.oak.spi.state.NodeState}.
 * For each of the supported properties a default is provided if either the
 * property doesn't exist in the node or if the value is <code>null</code> or
 * empty <code>String</code>.
 * <p/>
 * Subclasses of this should at least provide the {@link org.apache.jackrabbit.oak.spi.state.NodeState} which holds
 * the configuration.
 */
public abstract class OakSolrNodeStateConfiguration implements OakSolrConfiguration, SolrServerConfigurationProvider<EmbeddedSolrServerProvider> {

    /**
     * get the {@link org.apache.jackrabbit.oak.spi.state.NodeState} which contains the properties for the Oak -
     * Solr configuration.
     *
     * @return a (possibly non-existent) node state for the Solr configuration
     */
    protected abstract NodeState getConfigurationNodeState();

    @Override
    public String getFieldNameFor(Type<?> propertyType) {
        if (Type.BINARIES.equals(propertyType) || Type.BINARY.equals(propertyType)) {
            // TODO : use Tika / SolrCell here
            return propertyType.toString() + "_bin";
        }
        return null;
    }

    @Override
    public String getPathField() {
        return getStringValueFor(Properties.PATH_FIELD, SolrServerConfigurationDefaults.PATH_FIELD_NAME);
    }

    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = getStringValueFor(Properties.DESCENDANTS_FIELD, SolrServerConfigurationDefaults.DESC_FIELD_NAME);
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = getStringValueFor(Properties.CHILDREN_FIELD, SolrServerConfigurationDefaults.CHILD_FIELD_NAME);
                break;
            }
            case EXACT: {
                fieldName = getStringValueFor(Properties.PATH_FIELD, SolrServerConfigurationDefaults.PATH_FIELD_NAME);
                break;
            }
            case PARENT: {
                fieldName = getStringValueFor(Properties.PARENT_FIELD, SolrServerConfigurationDefaults.ANC_FIELD_NAME);
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
        return getStringValueFor(Properties.CATCHALL_FIELD, SolrServerConfigurationDefaults.CATCHALL_FIELD);
    }

    @Override
    public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
        return null;
    }

    @Override
    public CommitPolicy getCommitPolicy() {
        return CommitPolicy.valueOf(getStringValueFor(Properties.COMMIT_POLICY, CommitPolicy.SOFT.toString()));
    }

    @Override
    public int getRows() {
        return getIntValueFor(Properties.ROWS, SolrServerConfigurationDefaults.ROWS);
    }

    @Override
    public boolean useForPropertyRestrictions() {
        return getBooleanValueFor(Properties.PROPERTY_RESTRICIONS, SolrServerConfigurationDefaults.PROPERTY_RESTRICTIONS);
    }

    @Override
    public boolean useForPrimaryTypes() {
        return getBooleanValueFor(Properties.PRIMARY_TYPES, SolrServerConfigurationDefaults.PRIMARY_TYPES);
    }

    @Override
    public boolean useForPathRestrictions() {
        return getBooleanValueFor(Properties.PATH_RESTRICTIONS, SolrServerConfigurationDefaults.PATH_RESTRICTIONS);
    }

    @Override
    public Collection<String> getIgnoredProperties() {
        Collection<String> ignoredProperties;
        String ignoredPropertiesString = getStringValueFor(Properties.IGNORED_PROPERTIES, SolrServerConfigurationDefaults.IGNORED_PROPERTIES);
        if (ignoredPropertiesString != null) {
            ignoredProperties = Arrays.asList(ignoredPropertiesString.split(","));
        } else {
            ignoredProperties = Collections.emptyList();
        }
        return ignoredProperties;
    }

    private boolean getBooleanValueFor(String propertyName, boolean defaultValue) {
        boolean value = defaultValue;
        NodeState configurationNodeState = getConfigurationNodeState();
        if (configurationNodeState.exists()) {
            PropertyState property = configurationNodeState.getProperty(propertyName);
            if (property != null) {
                value = property.getValue(Type.BOOLEAN);
            }
        }
        return value;
    }

    private int getIntValueFor(String propertyName, int defaultValue) {
        long value = defaultValue;
        NodeState configurationNodeState = getConfigurationNodeState();
        if (configurationNodeState.exists()) {
            PropertyState property = configurationNodeState.getProperty(propertyName);
            if (property != null) {
                value = property.getValue(Type.LONG);
            }
        }
        return (int) value;
    }

    private String getStringValueFor(String propertyName, String defaultValue) {
        String value = defaultValue;
        NodeState configurationNodeState = getConfigurationNodeState();
        if (configurationNodeState.exists()) {
            PropertyState property = configurationNodeState.getProperty(propertyName);
            if (property != null) {
                value = property.getValue(Type.STRING);
            }
        }
        return value;
    }

    @Override
    public SolrServerConfiguration<EmbeddedSolrServerProvider> getSolrServerConfiguration() {
        String solrHomePath = getStringValueFor(Properties.SOLRHOME_PATH, SolrServerConfigurationDefaults.SOLR_HOME_PATH);
        String solrConfigPath = getStringValueFor(Properties.SOLRCONFIG_PATH, SolrServerConfigurationDefaults.SOLR_CONFIG_PATH);
        String coreName = getStringValueFor(Properties.CORE_NAME, SolrServerConfigurationDefaults.CORE_NAME);

        String context = getStringValueFor(Properties.CONTEXT, SolrServerConfigurationDefaults.CONTEXT);
        Integer httpPort = Integer.valueOf(getStringValueFor(Properties.HTTP_PORT, SolrServerConfigurationDefaults.HTTP_PORT));

        return new EmbeddedSolrServerConfiguration(solrHomePath,
                solrConfigPath, coreName).withHttpConfiguration(context, httpPort);
    }

    /**
     * Properties that may be retrieved from the configuration {@link org.apache.jackrabbit.oak.spi.state.NodeState}.
     */
    public final class Properties {
        public static final String SOLRHOME_PATH = "solrHomePath";
        public static final String SOLRCONFIG_PATH = "solrConfigPath";
        public static final String CONTEXT = "solrContext";
        public static final String HTTP_PORT = "httpPort";
        public static final String CORE_NAME = "coreName";
        public static final String PATH_FIELD = "pathField";
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
    }
}
