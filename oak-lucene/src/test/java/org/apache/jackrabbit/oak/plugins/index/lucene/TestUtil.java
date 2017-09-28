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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.Repository;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.IndexingMode;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class TestUtil {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static final String NT_TEST = "oak:TestNode";

    public static final String TEST_NODE_TYPE = "[oak:TestNode]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:TestNode VERSION";

    static void useV2(NodeBuilder idxNb) {
        if (!IndexFormatVersion.getDefault().isAtLeast(IndexFormatVersion.V2)) {
            idxNb.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        }
    }

    static void useV2(Tree idxTree) {
        if (!IndexFormatVersion.getDefault().isAtLeast(IndexFormatVersion.V2)) {
            idxTree.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        }
    }

    public static NodeBuilder newLuceneIndexDefinitionV2(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes) {
        NodeBuilder nb = LuceneIndexHelper.newLuceneIndexDefinition(index, name, propertyTypes, null, null, null);
        useV2(nb);
        return nb;
    }

    public static Tree enableForFullText(Tree props, String propName) {
        return enableForFullText(props, propName, false);
    }

    public static Tree enableForFullText(Tree props, String propName,  boolean regex) {
        Tree prop = props.addChild(unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, regex);
        prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        prop.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true);
        prop.setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true);
        return prop;
    }
    
    public static Tree enableForOrdered(Tree props, String propName) {
        Tree prop = enablePropertyIndex(props, propName, false);
        prop.setProperty("ordered", true);
        return prop;
    }

    public static Tree enablePropertyIndex(Tree props, String propName,  boolean regex) {
        Tree prop = props.addChild(unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, regex);
        prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, false);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, false);
        return prop;
    }

    public static Tree enableFunctionIndex(Tree props, String function) {
        Tree prop = props.addChild(unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_FUNCTION, function);
        return prop;
    }

    public static AggregatorBuilder newNodeAggregator(Tree indexDefn){
        return new AggregatorBuilder(indexDefn);
    }

    public static Tree newRulePropTree(Tree indexDefn, String typeName){
        Tree rules = indexDefn.addChild(LuceneIndexConstants.INDEX_RULES);
        rules.setOrderableChildren(true);
        Tree rule = rules.addChild(typeName);
        Tree props = rule.addChild(LuceneIndexConstants.PROP_NODE);
        props.setOrderableChildren(true);
        return props;
    }

    public static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    public static Document newDoc(String path){
        Document doc = new Document();
        doc.add(newPathField(path));
        return doc;
    }

    static class AggregatorBuilder {
        private final Tree aggs;

        private AggregatorBuilder(Tree indexDefn) {
            this.aggs = indexDefn.addChild(LuceneIndexConstants.AGGREGATES);
        }

        AggregatorBuilder newRuleWithName(String primaryType,
                                          List<String> includes){
            Tree agg = aggs.addChild(primaryType);
            for (String include : includes){
                agg.addChild(unique("include")).setProperty(LuceneIndexConstants.AGG_PATH, include);
            }
            return this;
        }
    }

    static String unique(String name){
        return name + COUNTER.getAndIncrement();
    }

    public static NodeBuilder registerTestNodeType(NodeBuilder builder){
        registerNodeType(builder, TEST_NODE_TYPE);
        return builder;
    }

    public static void registerNodeType(NodeBuilder builder, String nodeTypeDefn){
        //Taken from org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent
        NodeState base = ModifiedNodeState.squeeze(builder.getNodeState());
        NodeStore store = new MemoryNodeStore(base);
        Root root = RootFactory.createSystemRoot(
                store, new EditorHook(new CompositeEditorProvider(
                        new NamespaceEditorProvider(),
                        new TypeEditorProvider())), null, null, null);
        NodeTypeRegistry.register(root, IOUtils.toInputStream(nodeTypeDefn), "test node types");
        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }

    public static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    public static NodeBuilder createNodeWithType(NodeBuilder builder, String nodeName, String typeName){
        builder = builder.child(nodeName);
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return builder;
    }

    public static Tree createFileNode(Tree tree, String name, Blob content, String mimeType){
        Tree fileNode = tree.addChild(name);
        fileNode.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_FILE, Type.NAME);
        Tree jcrContent = fileNode.addChild(JCR_CONTENT);
        jcrContent.setProperty(JcrConstants.JCR_DATA, content);
        jcrContent.setProperty(JcrConstants.JCR_MIMETYPE, mimeType);
        return jcrContent;
    }

    public static Tree createFulltextIndex(Tree index, String name) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(createProperty(LuceneIndexConstants.INCLUDE_PROPERTY_TYPES,
                of(PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_BINARY), STRINGS));
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    public static void shutdown(Repository repository) {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    public static NodeBuilder enableIndexingMode(NodeBuilder builder, IndexingMode indexingMode){
        builder.setProperty(createAsyncProperty(indexingMode));
        return builder;
    }

    public static Tree enableIndexingMode(Tree tree, IndexingMode indexingMode){
        tree.setProperty(createAsyncProperty(indexingMode));
        return tree;
    }

    public static int createFile(Directory dir, String fileName, String content) throws IOException {
        byte[] data = content.getBytes();
        IndexOutput o = dir.createOutput(fileName, IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();
        return data.length;
    }

    private static PropertyState createAsyncProperty(String indexingMode) {
        return createProperty(IndexConstants.ASYNC_PROPERTY_NAME, of(indexingMode , "async"), STRINGS);
    }

    private static PropertyState createAsyncProperty(IndexingMode indexingMode) {
        switch(indexingMode) {
            case NRT  :
            case SYNC :
                return createAsyncProperty(indexingMode.asyncValueName());
            case ASYNC:
                return createProperty(IndexConstants.ASYNC_PROPERTY_NAME, of("async"), STRINGS);
            default:
                throw new IllegalArgumentException("Unknown mode " + indexingMode);
        }
    }

    public static class OptionalEditorProvider implements EditorProvider {
        public EditorProvider delegate;

        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info) throws CommitFailedException {
            if (delegate != null){
                return delegate.getRootEditor(before, after, builder, info);
            }
            return null;
        }


    }
}
