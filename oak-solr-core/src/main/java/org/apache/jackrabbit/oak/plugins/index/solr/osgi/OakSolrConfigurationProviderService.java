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
package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.metatype.annotations.Option;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider}
 */
@Component(
        immediate = true,
        service = { OakSolrConfigurationProvider.class }
)
@Designate(
        ocd = OakSolrConfigurationProviderService.Configuration.class
)
public class OakSolrConfigurationProviderService implements OakSolrConfigurationProvider {

    @ObjectClassDefinition(
            id = "org.apache.jackrabbit.oak.plugins.index.solr.osgi.OakSolrConfigurationProviderService",
            name = "Apache Jackrabbit Oak Solr indexing / search configuration" )
    @interface Configuration {
        @AttributeDefinition(
                name = "field for descendants search"
        )
        String path_desc_field() default OakSolrConfigurationDefaults.DESC_FIELD_NAME;

        @AttributeDefinition(
                name = "field for children search"
        )
        String path_child_field() default OakSolrConfigurationDefaults.CHILD_FIELD_NAME;

        @AttributeDefinition(
                name = "field for parent search"
        )
        String path_parent_field() default OakSolrConfigurationDefaults.ANC_FIELD_NAME;

        @AttributeDefinition(
                name = "field for path search"
        )
        String path_exact_field() default OakSolrConfigurationDefaults.PATH_FIELD_NAME;

        @AttributeDefinition(
                name = "catch all field"
        )
        String catch_all_field() default OakSolrConfigurationDefaults.CATCHALL_FIELD;

        @AttributeDefinition(
                name = "field for collapsing jcr:content paths"
        )
        String collapsed_path_field() default OakSolrConfigurationDefaults.COLLAPSED_PATH_FIELD;

        @AttributeDefinition(
                name = "field for path depth"
        )
        String path_depth_field() default OakSolrConfigurationDefaults.PATH_DEPTH_FIELD;

        @AttributeDefinition(
                name = "Property commit.policy",
                options = {
                        @Option(value = "HARD",
                                label = "Hard commit"
                        ),
                        @Option(value = "SOFT",
                                label = "Soft commit"
                        ),
                        @Option(value = "AUTO",
                                label = "Auto commit"
                        )}
        )
        OakSolrConfiguration.CommitPolicy commit_policy() default OakSolrConfiguration.CommitPolicy.SOFT;

        @AttributeDefinition(
                name = "rows"
        )
        int rows() default OakSolrConfigurationDefaults.ROWS;

        @AttributeDefinition(
                name = "path restrictions"
        )
        boolean path_restrictions() default OakSolrConfigurationDefaults.PATH_RESTRICTIONS;

        @AttributeDefinition(
                name = "property restrictions"
        )
        boolean property_restrictions() default OakSolrConfigurationDefaults.PROPERTY_RESTRICTIONS;

        @AttributeDefinition(
                name = "primary types restrictions"
        )
        boolean primarytypes_restrictions() default OakSolrConfigurationDefaults.PRIMARY_TYPES;

        @AttributeDefinition(
                name = "ignored properties"
        )
        String[] ignored_properties() default {"rep:members", "rep:authorizableId", "jcr:uuid", "rep:principalName", "rep:password"};

        @AttributeDefinition(
                name = "used properties"
        )
        String[] used_properties();

        @AttributeDefinition(
                name = "mappings from Oak Types to Solr fields",
                description = "each item should be in the form TypeString=FieldName (e.g. STRING=text_general)",
                cardinality = 13
        )
        String[] type_mappings() default OakSolrConfigurationDefaults.TYPE_MAPPINGS;

        @AttributeDefinition(
                name = "mappings from JCR property names to Solr fields",
                description = "each item should be in the form PropertyName=FieldName (e.g. jcr:title=text_en)"
        )
        String[] property_mappings() default OakSolrConfigurationDefaults.PROPERTY_MAPPINGS;

        @AttributeDefinition(
                name = "collapse jcr:content nodes"
        )
        boolean collapse_jcrcontent_nodes() default OakSolrConfigurationDefaults.COLLAPSE_JCR_CONTENT_NODES;

        @AttributeDefinition(
                name = "collapse jcr:content parents"
        )
        boolean collapse_jcrcontent_parents() default OakSolrConfigurationDefaults.COLLAPSE_JCR_CONTENT_PARENTS;
    }

    private String pathChildrenFieldName;
    private String pathParentFieldName;
    private String pathDescendantsFieldName;
    private String pathExactFieldName;
    private String collapsedPathField;
    private String catchAllField;
    private OakSolrConfiguration.CommitPolicy commitPolicy;
    private int rows;
    private boolean useForPathRestrictions;
    private boolean useForPropertyRestrictions;
    private boolean useForPrimaryTypes;
    private String[] ignoredProperties;
    private String[] usedProperties;
    private String[] typeMappings;
    private String[] propertyMappings;
    private boolean collapseJcrContentNodes;
    private boolean collapseJcrContentParents;
    private String depthField;

    private OakSolrConfiguration oakSolrConfiguration;

    @Activate
    protected void activate(Configuration configuration) {
        pathChildrenFieldName = configuration.path_child_field();
        pathParentFieldName = configuration.path_parent_field();
        pathExactFieldName = configuration.path_exact_field();
        collapsedPathField = configuration.collapsed_path_field();
        pathDescendantsFieldName = configuration.path_desc_field();
        catchAllField = configuration.catch_all_field();
        depthField = configuration.path_depth_field();
        rows = configuration.rows();
        commitPolicy = configuration.commit_policy();
        useForPathRestrictions = configuration.path_restrictions();
        useForPropertyRestrictions = configuration.property_restrictions();
        useForPrimaryTypes = configuration.primarytypes_restrictions();
        typeMappings = configuration.type_mappings();
        ignoredProperties = configuration.ignored_properties();
        usedProperties = configuration.used_properties();
        propertyMappings = configuration.property_mappings();
        collapseJcrContentNodes = configuration.collapse_jcrcontent_nodes();
        collapseJcrContentParents = configuration.collapse_jcrcontent_parents();
    }

    @Deactivate
    protected void deactivate() {
        oakSolrConfiguration = null;
    }

    @NotNull
    @Override
    public OakSolrConfiguration getConfiguration() {
        if (oakSolrConfiguration == null) {
            oakSolrConfiguration = new OakSolrConfiguration() {

                @Override
                public String getFieldNameFor(Type<?> propertyType) {
                    for (String typeMapping : typeMappings) {
                        String[] mapping = typeMapping.split("=");
                        if (mapping.length == 2 && mapping[0] != null && mapping[1] != null) {
                            Type<?> type = Type.fromString(mapping[0]);
                            if (type != null && type.tag() == propertyType.tag()) {
                                return mapping[1];
                            }
                        }
                    }
                    return null;
                }

                @Override
                public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction) {
                    for (String propertyMapping : propertyMappings) {
                        String[] mapping = propertyMapping.split("=");
                        if (mapping.length == 2 && mapping[0] != null && mapping[1] != null) {
                            if (mapping[0].equals(propertyRestriction.propertyName)) {
                                return mapping[1];
                            }
                        }
                    }
                    return null;
                }

                @NotNull
                @Override
                public String getPathField() {
                    return pathExactFieldName;
                }

                @Nullable
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

                @NotNull
                @Override
                public CommitPolicy getCommitPolicy() {
                    return commitPolicy;
                }

                @Override
                public String getCatchAllField() {
                    return catchAllField;
                }

                @Override
                public int getRows() {
                    return rows;
                }

                @Override
                public boolean useForPropertyRestrictions() {
                    return useForPropertyRestrictions;
                }

                @Override
                public boolean useForPrimaryTypes() {
                    return useForPrimaryTypes;
                }

                @Override
                public boolean useForPathRestrictions() {
                    return useForPathRestrictions;
                }

                @NotNull
                @Override
                public Collection<String> getIgnoredProperties() {
                    if (ignoredProperties != null && ignoredProperties.length > 0 && ignoredProperties[0].length() > 0) {
                        return Arrays.asList(ignoredProperties);
                    } else {
                        return Collections.emptyList();
                    }
                }

                @NotNull
                @Override
                public Collection<String> getUsedProperties() {
                    if (usedProperties != null && usedProperties.length > 0 && usedProperties[0].length() > 0) {
                        return Arrays.asList(usedProperties);
                    } else {
                        return Collections.emptyList();
                    }
                }

                @Override
                public boolean collapseJcrContentNodes() {
                    return collapseJcrContentNodes;
                }

                @Override
                public boolean collapseJcrContentParents() {
                    return collapseJcrContentParents;
                }

                @NotNull
                @Override
                public String getCollapsedPathField() {
                    return collapsedPathField;
                }

                @Override
                public String getPathDepthField() {
                    return depthField;
                }
            };
        }
        return oakSolrConfiguration;
    }
}
