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
 * A Solr configuration holding all the possible customizable parameters that
 * can be leveraged for an Oak search index.
 */
public interface OakSolrConfiguration {

    /**
     * Provide a field name to be used for indexing / searching a certain {@link org.apache.jackrabbit.oak.api.Type}
     *
     * @param propertyType the {@link org.apache.jackrabbit.oak.api.Type} to be indexed / searched
     * @return a <code>String</code> representing the Solr field to be used for the given {@link org.apache.jackrabbit.oak.api.Type}.
     */
    public String getFieldNameFor(Type<?> propertyType);

    /**
     * Provide the field name for indexing / searching paths
     *
     * @return a <code>String</code> representing the Solr field to be used for paths.
     */
    public String getPathField();

    /**
     * Provide a field name to search over for the given {@link org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction}
     *
     * @param pathRestriction the {@link org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction} used for filtering search results
     * @return the field name as a <code>String</code> to be used by Solr for the given restriction
     */
    public String getFieldForPathRestriction(Filter.PathRestriction pathRestriction);

    /**
     * Provide a field name to search over for the given {@link org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction}
     *
     * @param propertyRestriction the {@link org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction} used for filtering search results
     * @return the field name as a <code>String</code> to be used by Solr for the given restriction
     */
    public String getFieldForPropertyRestriction(Filter.PropertyRestriction propertyRestriction);

    /**
     * Provide the commit policy to be used with the underlying Solr instance
     *
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy}
     */
    public CommitPolicy getCommitPolicy();

    /**
     * Provide a field name that is used as the default "catch all" field for searching over all the data
     * @return a <code>String</code> representing the Solr field to be used as "catch all" field
     */
    public String getCatchAllField();
}
