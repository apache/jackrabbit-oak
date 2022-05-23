package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlatFileSplitterTest {

    @Test
    public void split() {
//        FlatFileSplitter splitter = new FlatFileSplitter() {
//        };
    }

    @Test
    public void getSplitNodeTypeNames() throws IOException, CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        EditorHook hook = new EditorHook(
                new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider()));
        OakInitializer.initialize(store, new InitialContent(), hook);

        Set<IndexDefinition> defns = new HashSet<>();

        IndexDefinitionBuilder defnb1 = new IndexDefinitionBuilder();
        defnb1.indexRule("testIndexRule1");
        defnb1.aggregateRule("testAggregate1");
        IndexDefinition defn1 = IndexDefinition.newBuilder(store.getRoot(), defnb1.build(), "/foo").build();
        defns.add(defn1);

        IndexDefinitionBuilder defnb2 = new IndexDefinitionBuilder();
        defnb2.indexRule("testIndexRule2");
        defnb2.aggregateRule("testAggregate2");
        defnb2.aggregateRule("testAggregate3");
        IndexDefinition defn2 = IndexDefinition.newBuilder(store.getRoot(), defnb2.build(), "/bar").build();
        defns.add(defn2);

        List<String> resultNodeTypes = new ArrayList<>();
        NodeTypeInfoProvider mockNodeTypeInfoProvider = Mockito.mock(NodeTypeInfoProvider.class);
        for (String nodeType: new ArrayList<String>(Arrays.asList(
                "testIndexRule1",
                "testIndexRule2",
                "testAggregate1",
                "testAggregate2",
                "testAggregate3"
        ))) {
            NodeTypeInfo mockNodeTypeInfo = Mockito.mock(NodeTypeInfo.class, nodeType);
            Mockito.when(mockNodeTypeInfo.getNodeTypeName()).thenReturn(nodeType);
            Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo(nodeType)).thenReturn(mockNodeTypeInfo);

            String primary1 = nodeType + "TestPrimarySubType";
            Set<String> primary = new HashSet<>(Arrays.asList(primary1));
            Mockito.when(mockNodeTypeInfo.getPrimarySubTypes()).thenReturn(primary);
            NodeTypeInfo mockPrimary = Mockito.mock(NodeTypeInfo.class, nodeType);
            Mockito.when(mockPrimary.getPrimarySubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockPrimary.getMixinSubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockPrimary.getNodeTypeName()).thenReturn(primary1);
            Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo(primary1)).thenReturn(mockPrimary);

            String mixin1 = nodeType+"TestMixinSubType1";
            String mixin2 = nodeType+"TestMixinSubType2";
            Set<String> mixin = new HashSet<>(Arrays.asList(mixin1, mixin2));
            Mockito.when(mockNodeTypeInfo.getMixinSubTypes()).thenReturn(mixin);
            NodeTypeInfo mockMixin1 = Mockito.mock(NodeTypeInfo.class, nodeType);
            Mockito.when(mockMixin1.getPrimarySubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockMixin1.getMixinSubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockMixin1.getNodeTypeName()).thenReturn(mixin1);
            Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo(mixin1)).thenReturn(mockMixin1);
            NodeTypeInfo mockMixin2 = Mockito.mock(NodeTypeInfo.class, nodeType);
            Mockito.when(mockMixin2.getPrimarySubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockMixin2.getMixinSubTypes()).thenReturn(new HashSet<>());
            Mockito.when(mockMixin2.getNodeTypeName()).thenReturn(mixin2);
            Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo(mixin2)).thenReturn(mockMixin2);


            resultNodeTypes.add(nodeType);
            resultNodeTypes.addAll(primary);
            resultNodeTypes.addAll(mixin);
        }
        assertEquals("test setup incorrectly", resultNodeTypes.size(), 20);

        FlatFileSplitter splitter = new FlatFileSplitter(null, null, null, mockNodeTypeInfoProvider, null, defns);
        Set<String> nodeTypes = splitter.getSplitNodeTypeNames();

        assertEquals(resultNodeTypes.size(), nodeTypes.size());
        assertTrue(nodeTypes.containsAll(resultNodeTypes));
    }
}