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
package org.apache.jackrabbit.oak.plugins.index.solr.server;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An {@link OakSolrConfiguration} specified via a given {@link NodeState}.
 * For each of the supported properties a default is provided if either the
 * property doesn't exist in the node or if the value is <code>null</code> or
 * empty <code>String</code>.
 */
public class OakSolrNodeStateConfiguration implements OakSolrConfiguration {

    private NodeState solrConfigurationNodeState;

    public OakSolrNodeStateConfiguration(NodeState solrConfigurationNodeState) {
        this.solrConfigurationNodeState = solrConfigurationNodeState;
    }

    @Override
    public String getFieldNameFor(Type<?> propertyType) {
        return null;
    }

    @Override
    public String getPathField() {
        return getStringValueFor(Properties.PATH_FIELD, "path_exact");
    }

    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = getStringValueFor(Properties.DESCENDANTS_FIELD, "path_des");
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = getStringValueFor(Properties.CHILDREN_FIELD, "path_child");
                break;
            }
            case EXACT: {
                fieldName = getStringValueFor(Properties.PATH_FIELD, "path_exact");
                break;
            }
            case PARENT: {
                fieldName = getStringValueFor(Properties.PARENT_FIELD, "path_anc");
                break;
            }

        }
        return fieldName;
    }

    @Override
    public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
        return null;
    }

    @Override
    public CommitPolicy getCommitPolicy() {
        return CommitPolicy.valueOf(getStringValueFor(Properties.COMMIT_POLICY, CommitPolicy.HARD.toString()));
    }

    public String getSolrHomePath() {
        return getStringValueFor(Properties.SOLRHOME_PATH, "./");
    }

    public String getSolrConfigPath() {
        return getStringValueFor(Properties.SOLRCONFIG_PATH, "./solr.xml");
    }

    @Override
    public String getCoreName() {
        return getStringValueFor(Properties.CORE_NAME, "oak");
    }

    private String getStringValueFor(String propertyName, String defaultValue) {
        String value = null;
        PropertyState property = solrConfigurationNodeState.getProperty(propertyName);
        if (property != null) {
            value = property.getValue(Type.STRING);
        }
        if (value == null || value.length() == 0) {
            value = defaultValue;
        }
        return value;
    }

    /**
     * Properties that may be retrieved from the configuration {@link NodeState}.
     */
    public final class Properties {

        public static final String SOLRHOME_PATH = "solrHomePath";
        public static final String SOLRCONFIG_PATH = "solrConfigPath";
        public static final String CORE_NAME = "coreName";
        public static final String PATH_FIELD = "pathField";
        public static final String PARENT_FIELD = "parentField";
        public static final String CHILDREN_FIELD = "childrenField";
        public static final String DESCENDANTS_FIELD = "descendantsField";
        public static final String COMMIT_POLICY = "commitPolicy";

    }
}
