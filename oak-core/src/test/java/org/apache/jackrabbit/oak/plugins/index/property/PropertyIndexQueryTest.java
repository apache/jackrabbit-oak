/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Tests the query engine using the default index implementation: the
 * {@link PropertyIndexProvider}
 */
public class PropertyIndexQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return getOakRepositoryInstance().createContentRepository();
    }

    /**
     * return an instance of {@link Oak} repository ready to be built with
     * {@link Oak#createContentRepository()}.
     * 
     * @return
     */
    @Nonnull
    Oak getOakRepositoryInstance() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new PropertyIndexProvider())
            .with(new PropertyIndexEditorProvider());
    }
    
    @Test
    public void nativeQuery() throws Exception {
        test("sql2_native.txt");
    }

    @Test
    public void xpath() throws Exception {
        test("xpath.txt");
    }

    @Ignore("OAK-1517")
    @Test
    public void testInvalidNamespace() throws Exception {
        new Oak().with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new RepositoryInitializer(){

                    @Override
                    public void initialize(@Nonnull NodeBuilder builder) {
                        createIndexDefinition(
                                builder.child(INDEX_DEFINITIONS_NAME),
                                "foo",
                                true,
                                false,
                                ImmutableSet.of("illegal:namespaceProperty"), null);
                    }
                })
                .createContentRepository();
        fail("creating an index definition with an illegal namespace should fail.");
    }

    @Test
    public void bindVariableTest() throws Exception {
        Tree tree = root.getTree("/");
        Tree test = tree.addChild("test");
        test.addChild("hello").setProperty("id", "1");
        test.addChild("world").setProperty("id", "2");
        root.commit();

        Map<String, PropertyValue> sv = new HashMap<String, PropertyValue>();
        sv.put("id", PropertyValues.newString("1"));
        Iterator<? extends ResultRow> result;
        result = executeQuery("select * from [nt:base] where id = $id",
                SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/hello", result.next().getPath());

        sv.put("id", PropertyValues.newString("2"));
        result = executeQuery("select * from [nt:base] where id = $id",
                SQL2, sv).getRows().iterator();
        assertTrue(result.hasNext());
        assertEquals("/test/world", result.next().getPath());
    }

    @Test
    public void sql2Index() throws Exception {
        test("sql2_index.txt");
    }

    @Test
    public void sql2Measure() throws Exception {
        test("sql2_measure.txt");
    }

    @Test
    public void sql1() throws Exception {
        test("sql1.txt");
    }

    @Test
    public void sql2() throws Exception {
        test("sql2.txt");
    }

}