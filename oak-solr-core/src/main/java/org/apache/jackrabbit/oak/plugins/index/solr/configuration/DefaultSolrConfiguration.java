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

import java.util.Collection;
import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * Default {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration}
 */
public class DefaultSolrConfiguration implements OakSolrConfiguration {

    private static final Collection<String> ignoredProperties = OakSolrConfigurationDefaults.IGNORED_PROPERTIES;
    private static final Collection<String> usedProperties = Collections.emptyList();

    @Override
    public String getFieldNameFor(Type<?> propertyType) {
        return null;
    }

    @Nonnull
    @Override
    public String getPathField() {
        return OakSolrConfigurationDefaults.PATH_FIELD_NAME;
    }

    @CheckForNull
    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = OakSolrConfigurationDefaults.DESC_FIELD_NAME;
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = OakSolrConfigurationDefaults.CHILD_FIELD_NAME;
                break;
            }
            case EXACT: {
                fieldName = OakSolrConfigurationDefaults.PATH_FIELD_NAME;
                break;
            }
            case PARENT: {
                fieldName = OakSolrConfigurationDefaults.ANC_FIELD_NAME;
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
    public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
        return null;
    }

    @Nonnull
    @Override
    public CommitPolicy getCommitPolicy() {
        return CommitPolicy.SOFT;
    }

    @Override
    public String getCatchAllField() {
        return OakSolrConfigurationDefaults.CATCHALL_FIELD;
    }

    @Override
    public int getRows() {
        return OakSolrConfigurationDefaults.ROWS;
    }

    @Override
    public boolean useForPropertyRestrictions() {
        return OakSolrConfigurationDefaults.PROPERTY_RESTRICTIONS;
    }

    @Override
    public boolean useForPrimaryTypes() {
        return OakSolrConfigurationDefaults.PRIMARY_TYPES;
    }

    @Override
    public boolean useForPathRestrictions() {
        return OakSolrConfigurationDefaults.PATH_RESTRICTIONS;
    }

    @Nonnull
    @Override
    public Collection<String> getIgnoredProperties() {
        return ignoredProperties;
    }

    @Nonnull
    @Override
    public Collection<String> getUsedProperties() {
        return usedProperties;
    }

    @Override
    public boolean collapseJcrContentNodes() {
        return OakSolrConfigurationDefaults.COLLAPSE_JCR_CONTENT_NODES;
    }

    @Nonnull
    @Override
    public String getCollapsedPathField() {
        return OakSolrConfigurationDefaults.COLLAPSED_PATH_FIELD;
    }

    @Override
    public String getPathDepthField() {
        return OakSolrConfigurationDefaults.PATH_DEPTH_FIELD;
    }

}
