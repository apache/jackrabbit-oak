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
package org.apache.jackrabbit.oak.plugins.index.solr.http;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.osgi.service.component.ComponentContext;

/**
 * An {@link OakSolrConfigurationProvider} for the remote Solr server
 * <p/>
 * In this {@link OakSolrConfiguration} the 'path' related fields are taken from
 * OSGi configuration while the other configuration just does nothing triggering
 * the default behavior that properties are indexed by name.
 * Possible extensions of this class may trigger type based property indexing / search.
 */
@Component(metatype = true, immediate = true, label = "Remote Solr Configuration Provider")
@Service(OakSolrConfigurationProvider.class)
public class RemoteSolrConfigurationProvider implements OakSolrConfigurationProvider {

    private static final String DEFAULT_DESC_FIELD = "path_des";
    private static final String DEFAULT_CHILD_FIELD = "path_child";
    private static final String DEFAULT_PARENT_FIELD = "path_anc";
    private static final String DEFAULT_PATH_FIELD = "path_exact";

    @Property(value = DEFAULT_DESC_FIELD, label = "field for descendants search")
    private static final String PATH_DESCENDANTS_FIELD = "path.desc.field";

    @Property(value = DEFAULT_CHILD_FIELD, label = "field for children search")
    private static final String PATH_CHILDREN_FIELD = "path.child.field";

    @Property(value = DEFAULT_PARENT_FIELD, label = "field for parent search")
    private static final String PATH_PARENT_FIELD = "path.parent.field";

    @Property(value = DEFAULT_PATH_FIELD, label = "field for path search")
    private static final String PATH_EXACT_FIELD = "path.exact.field";

    private String pathChildrenFieldName;
    private String pathParentFieldName;
    private String pathDescendantsFieldName;
    private String pathExactFieldName;

    private OakSolrConfiguration oakSolrConfiguration;

    public RemoteSolrConfigurationProvider() {
        this.pathChildrenFieldName = DEFAULT_CHILD_FIELD;
        this.pathDescendantsFieldName = DEFAULT_DESC_FIELD;
        this.pathExactFieldName = DEFAULT_PATH_FIELD;
        this.pathParentFieldName = DEFAULT_PARENT_FIELD;
    }

    public RemoteSolrConfigurationProvider(String pathChildrenFieldName, String pathParentFieldName,
                                           String pathDescendantsFieldName, String pathExactFieldName) {
        this.pathChildrenFieldName = pathChildrenFieldName;
        this.pathParentFieldName = pathParentFieldName;
        this.pathDescendantsFieldName = pathDescendantsFieldName;
        this.pathExactFieldName = pathExactFieldName;
    }

    protected void activate(ComponentContext componentContext) throws Exception {
        pathChildrenFieldName = String.valueOf(componentContext.getProperties().get(PATH_CHILDREN_FIELD));
        pathParentFieldName = String.valueOf(componentContext.getProperties().get(PATH_PARENT_FIELD));
        pathExactFieldName = String.valueOf(componentContext.getProperties().get(PATH_EXACT_FIELD));
        pathDescendantsFieldName = String.valueOf(componentContext.getProperties().get(PATH_DESCENDANTS_FIELD));
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        if (oakSolrConfiguration == null) {
            oakSolrConfiguration = new OakSolrConfiguration() {
                @Override
                public String getFieldNameFor(Type<?> propertyType) {
                    if (Type.BINARIES.equals(propertyType) || Type.BINARY.equals(propertyType)) {
                        // TODO : use Tika / SolrCell here
                        return propertyType.toString()+"_bin";
                    }
                    return null;
                }

                @Override
                public String getPathField() {
                    return pathExactFieldName;
                }

                @Override
                public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction) {
                    String fieldName = null;
                    switch (pathRestriction) {
                        case ALL_CHILDREN: {
                            fieldName = pathDescendantsFieldName;
                            break;
                        }
                        case DIRECT_CHILDREN: {
                            fieldName = pathChildrenFieldName;
                            break;
                        }
                        case EXACT: {
                            fieldName = pathExactFieldName;
                            break;
                        }
                        case PARENT: {
                            fieldName = pathParentFieldName;
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

            };
        }
        return oakSolrConfiguration;
    }
}
