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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.codecs.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ORDERED_PROP_NAMES;

public class IndexDefinition {
    private static final Logger log = LoggerFactory.getLogger(IndexDefinition.class);

    /**
     * Blob size to use by default. To avoid issues in OAK-2105 the size should not
     * be power of 2.
     */
    static final int DEFAULT_BLOB_SIZE = OakDirectory.DEFAULT_BLOB_SIZE - 300;

    private final int propertyTypes;

    private final Set<String> excludes;

    private final Set<String> includes;

    private final Set<String> orderedProps;

    private final boolean fullTextEnabled;

    private final boolean storageEnabled;

    private final NodeBuilder definition;

    private final Map<String, PropertyDefinition> propDefns;

    private final String funcName;

    private final int blobSize;

    private final Codec codec;

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
        this.orderedProps = getMultiProperty(defn, ORDERED_PROP_NAMES);
        this.blobSize = getOptionalValue(defn, BLOB_SIZE, DEFAULT_BLOB_SIZE);

        this.fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        //Storage is disabled for non full text indexes
        this.storageEnabled = this.fullTextEnabled && getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);

        Map<String, PropertyDefinition> propDefns = Maps.newHashMap();
        NodeBuilder propNode = defn.getChildNode(LuceneIndexConstants.PROP_NODE);
        for(String propName : Iterables.concat(includes, orderedProps)){
            if(propNode.hasChildNode(propName)){
                propDefns.put(propName, new PropertyDefinition(this, propName, propNode.child(propName)));
            }
        }
        this.propDefns = ImmutableMap.copyOf(propDefns);

        String functionName = getOptionalValue(defn, LuceneIndexConstants.FUNC_NAME, null);
        this.funcName = functionName != null ? "native*" + functionName : null;

        this.codec = createCodec();
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

    boolean isOrdered(String name) {
        return orderedProps.contains(name);
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

    public String getFunctionName(){
        return funcName;
    }

    public boolean hasFunctionDefined(){
        return funcName != null;
    }

    /**
     * Size in bytes for the blobs created while storing the index content
     * @return size in bytes
     */
    public int getBlobSize() {
        return blobSize;
    }

    public Codec getCodec() {
        return codec;
    }

    //~------------------------------------------< Internal >

    private Codec createCodec() {
        String codecName = getOptionalValue(definition, LuceneIndexConstants.CODEC_NAME, null);
        Codec codec = null;
        if (codecName != null) {
            codec = Codec.forName(codecName);
        } else if (fullTextEnabled) {
            codec = new OakCodec();
        }
        return codec;
    }

    private static boolean getOptionalValue(NodeBuilder definition, String propName, boolean defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.BOOLEAN);
    }

    private static int getOptionalValue(NodeBuilder definition, String propName, int defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : Ints.checkedCast(ps.getValue(Type.LONG));
    }

    private static String getOptionalValue(NodeBuilder definition, String propName, String defaultVal){
        PropertyState ps = definition.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.STRING);
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
