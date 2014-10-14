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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;

public class IndexDefinition {
    private static final Logger log = LoggerFactory.getLogger(IndexDefinition.class);
    private final int propertyTypes;

    private final Set<String> excludes;

    private final Set<String> includes;

    private final boolean fullTextEnabled;

    private final boolean storageEnabled;

    private final NodeBuilder definition;

    private final Map<String, PropertyDefinition> propDefns;

    public IndexDefinition(NodeBuilder defn) {
        this.definition = defn;
        PropertyState pst = defn.getProperty(INCLUDE_PROPERTY_TYPES);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            this.propertyTypes = types;
        } else {
            this.propertyTypes = -1;
        }

        this.excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        this.includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        this.fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        //Storage is disabled for non full text indexes
        this.storageEnabled = this.fullTextEnabled && getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);

        Map<String, PropertyDefinition> propDefns = Maps.newHashMap();
        for(String propName : includes){
            if(defn.hasChildNode(propName)){
                propDefns.put(propName, new PropertyDefinition(this, propName, defn.child(propName)));
            }
        }
        this.propDefns = ImmutableMap.copyOf(propDefns);
    }

    boolean includeProperty(String name) {
        if(!includes.isEmpty()){
            return includes.contains(name);
        }
        return !excludes.contains(name.toLowerCase());
    }

    boolean includePropertyType(int type){
        if(propertyTypes < 0){
            return false;
        }
        return (propertyTypes & (1 << type)) != 0;
    }

    public NodeBuilder getDefinition() {
        return definition;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public int getPropertyTypes() {
        return propertyTypes;
    }

    /**
     * Checks if a given property should be stored in the lucene index or not
     */
    public boolean isStored(String name) {
        return storageEnabled;
    }

    public boolean skipTokenization(String propertyName) {
        //If fulltext is not enabled then we never tokenize
        //irrespective of property name
        if (!isFullTextEnabled()) {
            return true;
        }
        return LuceneIndexHelper.skipTokenization(propertyName);
    }

    @CheckForNull
    public PropertyDefinition getPropDefn(String propName){
        return propDefns.get(propName);
    }

    public boolean hasPropertyDefinition(String propName){
        return propDefns.containsKey(propName);
    }

    //~------------------------------------------< Internal >

    private static boolean getOptionalValue(NodeBuilder definition, String propName, boolean defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.BOOLEAN);
    }

    private static Set<String> getMultiProperty(NodeBuilder definition, String propName){
        PropertyState pse = definition.getProperty(propName);
        return pse != null ? ImmutableSet.copyOf(pse.getValue(Type.STRINGS)) : Collections.<String>emptySet();
    }

    private static Set<String> toLowerCase(Set<String> values){
        Set<String> result = Sets.newHashSet();
        for(String val : values){
            result.add(val.toLowerCase());
        }
        return Collections.unmodifiableSet(result);
    }
}
