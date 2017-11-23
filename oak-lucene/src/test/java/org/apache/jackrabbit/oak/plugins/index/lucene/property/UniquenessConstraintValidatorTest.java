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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.PropertyUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.child;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class UniquenessConstraintValidatorTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private IndexDefinitionBuilder defnb = new IndexDefinitionBuilder();
    private String indexPath  = "/oak:index/foo";

    @Test
    public void singleUniqueProperty() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        updateRootState("/a", "foo", "bar");
        updateRootState("/b", "foo", "bar");

        PropertyUpdateCallback callback = newCallback();

        callback.propertyUpdated("/a", "foo", pd("foo"),
                null, createProperty("foo", "bar"));
        callback.propertyUpdated("/b", "foo", pd("foo"),
                null, createProperty("foo", "bar"));
        try {
            callback.done();
            fail();
        } catch (CommitFailedException e) {
            assertEquals(CONSTRAINT, e.getType());
            assertEquals(30, e.getCode());

            assertThat(e.getMessage(), containsString(indexPath));
            assertThat(e.getMessage(), containsString("/a"));
            assertThat(e.getMessage(), containsString("/b"));
            assertThat(e.getMessage(), containsString("foo"));
        }
    }

    @Test
    public void multipleUniqueProperties() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();
        defnb.indexRule("nt:base").property("foo2").unique();

        PropertyUpdateCallback callback = newCallback();

        propertyUpdated(callback, "/a", "foo", "bar");
        propertyUpdated(callback, "/a", "foo2", "bar");

        //As properties are different this should pass
        callback.done();
    }

    @Test(expected = CommitFailedException.class)
    public void firstStore_PreExist() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        updateRootState("/a", "foo", "bar");

        PropertyUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        refreshBuilder();
        updateRootState("/b", "foo", "bar");

        callback = newCallback();
        propertyUpdated(callback, "/b", "foo", "bar");
        callback.done();
    }


    @Test
    public void secondStore_SamePath() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/a"));

        //Should work as paths for unique property are same
        callback.done();
    }

    @Test(expected = CommitFailedException.class)
    public void secondStore_DiffPath() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        updateRootState("/b", "foo", "bar");
        updateRootState("/a", "foo", "bar");

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/b"));

        callback.done();
    }

    @Test
    public void secondStore_NodeNotExist() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/b"));

        callback.done();
    }

    @Test
    public void secondStore_NodeExist_PropertyNotExist() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        NodeBuilder rootBuilder = root.builder();
        rootBuilder.child("b");
        root = rootBuilder.getNodeState();

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/b"));

        callback.done();
    }

    @Test
    public void secondStore_NodeExist_PropertyExist_DifferentValue() throws Exception{
        defnb.indexRule("nt:base").property("foo").unique();

        NodeBuilder rootBuilder = root.builder();
        rootBuilder.child("b").setProperty("foo", "bar2");
        root = rootBuilder.getNodeState();

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/b"));

        callback.done();
    }

    @Test(expected = CommitFailedException.class)
    public void secondStore_RelativeProperty() throws Exception{
        defnb.indexRule("nt:base").property("jcr:content/foo").unique();

        updateRootState("/b/jcr:content", "foo", "bar");
        updateRootState("/a/jcr:content", "foo", "bar");

        PropertyIndexUpdateCallback callback = newCallback();
        propertyUpdated(callback, "/a", "jcr:content/foo", "bar");

        callback.getUniquenessConstraintValidator()
                .setSecondStore((propertyRelativePath, value) -> singletonList("/b"));

        callback.done();
    }

    private void propertyUpdated(PropertyUpdateCallback callback, String nodePath, String propertyName, String value){
        callback.propertyUpdated(nodePath, propertyName, pd(propertyName),
                null, createProperty(propertyName, value));
    }

    private PropertyIndexUpdateCallback newCallback(){
        return new PropertyIndexUpdateCallback(indexPath, builder, root);
    }

    private void updateRootState(String nodePath, String propertyName, String value) {
        NodeBuilder rootBuilder = root.builder();
        child(rootBuilder, nodePath).setProperty(propertyName, value);
        root = rootBuilder.getNodeState();
    }

    private void refreshBuilder() {
        builder = builder.getNodeState().builder();
    }

    private PropertyDefinition pd(String propName){
        IndexDefinition defn = new IndexDefinition(root, defnb.build(), indexPath);
        return defn.getApplicableIndexingRule("nt:base").getConfig(propName);
    }
}