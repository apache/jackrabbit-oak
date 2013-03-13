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
package org.apache.jackrabbit.oak.plugins.index.solr.embedded;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;

/**
 * An {@link OakSolrConfiguration} for the embedded Solr server
 */
public class EmbeddedSolrConfiguration implements OakSolrConfiguration {

    private static final String PATH_FIELD_NAME = "path";
    private static final String CHILD_FIELD_NAME = "path_child";
    private static final String DESC_FIELD_NAME = "path_desc";
    private static final String ANC_FIELD_NAME = "path_anc";

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
        return PATH_FIELD_NAME;
    }

    @Override
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
        String fieldName = null;
        switch (pathRestriction) {
            case ALL_CHILDREN: {
                fieldName = DESC_FIELD_NAME;
                break;
            }
            case DIRECT_CHILDREN: {
                fieldName = CHILD_FIELD_NAME;
                break;
            }
            case EXACT: {
                fieldName = PATH_FIELD_NAME;
                break;
            }
            case PARENT: {
                fieldName = ANC_FIELD_NAME;
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
        return CommitPolicy.SOFT;
    }

    @Override
    public String getCoreName() {
        return "oak";
    }
}
