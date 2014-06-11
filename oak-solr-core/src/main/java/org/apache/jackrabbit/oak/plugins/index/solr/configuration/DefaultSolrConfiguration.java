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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * Default {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration}
 */
public class DefaultSolrConfiguration implements OakSolrConfiguration {

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
        return SolrServerConfigurationDefaults.PATH_FIELD_NAME;
    }

    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = SolrServerConfigurationDefaults.DESC_FIELD_NAME;
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = SolrServerConfigurationDefaults.CHILD_FIELD_NAME;
                break;
            }
            case EXACT: {
                fieldName = SolrServerConfigurationDefaults.PATH_FIELD_NAME;
                break;
            }
            case PARENT: {
                fieldName = SolrServerConfigurationDefaults.ANC_FIELD_NAME;
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

    @Override
    public CommitPolicy getCommitPolicy() {
        return CommitPolicy.SOFT;
    }

    @Override
    public String getCatchAllField() {
        return SolrServerConfigurationDefaults.CATCHALL_FIELD;
    }

    @Override
    public int getRows() {
        return SolrServerConfigurationDefaults.ROWS;
    }

    @Override
    public boolean useForPropertyRestrictions() {
        return SolrServerConfigurationDefaults.PROPERTY_RESTRICTIONS;
    }

    @Override
    public boolean useForPrimaryTypes() {
        return SolrServerConfigurationDefaults.PRIMARY_TYPES;
    }

    @Override
    public boolean useForPathRestrictions() {
        return SolrServerConfigurationDefaults.PATH_RESTRICTIONS;
    }

}
