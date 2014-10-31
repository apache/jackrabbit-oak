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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.codecs.Codec;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;
import static javax.jcr.PropertyType.TYPENAME_LONG;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexDefinitionTest {
    private Codec oakCodec = new OakCodec();

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void fullTextEnabled() throws Exception{
        IndexDefinition defn = new IndexDefinition(builder);
        assertTrue("By default fulltext is enabled", defn.isFullTextEnabled());
        assertTrue("By default everything is indexed", defn.includeProperty("foo"));
        assertFalse("Property types need to be defined", defn.includePropertyType(PropertyType.DATE));
        assertTrue("For fulltext storage is enabled", defn.isStored("foo"));

        assertFalse(defn.skipTokenization("foo"));
        assertTrue(defn.skipTokenization("jcr:uuid"));
    }

    @Test
    public void propertyTypes() throws Exception{
        builder.setProperty(createProperty(INCLUDE_PROPERTY_TYPES, of(TYPENAME_LONG), STRINGS));
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        IndexDefinition defn = new IndexDefinition(builder);

        assertFalse(defn.isFullTextEnabled());
        assertFalse("If fulltext disabled then nothing stored",defn.isStored("foo"));

        assertTrue(defn.includePropertyType(PropertyType.LONG));
        assertFalse(defn.includePropertyType(PropertyType.STRING));

        assertTrue(defn.includeProperty("foo"));
        assertTrue(defn.includeProperty("bar"));
        assertFalse(defn.includeProperty("baz"));

        assertTrue(defn.skipTokenization("foo"));
    }

    @Test
    public void propertyDefinition() throws Exception{
        builder.child("foo").setProperty(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
        builder.setProperty(createProperty(INCLUDE_PROPERTY_NAMES, of("foo" , "bar"), STRINGS));
        IndexDefinition defn = new IndexDefinition(builder);

        assertTrue(defn.hasPropertyDefinition("foo"));
        assertFalse(defn.hasPropertyDefinition("bar"));

        assertEquals(PropertyType.DATE, defn.getPropDefn("foo").getPropertyType());
    }

    @Test
    public void codecConfig() throws Exception{
        IndexDefinition defn = new IndexDefinition(builder);
        assertNotNull(defn.getCodec());
        assertEquals(oakCodec.getName(), defn.getCodec().getName());

        builder.setProperty(LuceneIndexConstants.FULL_TEXT_ENABLED, false);
        defn = new IndexDefinition(builder);
        assertNull(defn.getCodec());

        Codec simple = Codec.getDefault();
        builder.setProperty(LuceneIndexConstants.CODEC_NAME, simple.getName());
        defn = new IndexDefinition(builder);
        assertNotNull(defn.getCodec());
        assertEquals(simple.getName(), defn.getCodec().getName());
    }


}
