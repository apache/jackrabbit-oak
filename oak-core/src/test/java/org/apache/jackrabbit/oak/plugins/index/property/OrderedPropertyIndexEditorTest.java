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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class OrderedPropertyIndexEditorTest {
   
   @Test public void isProperlyConfiguredWithPropertyNames(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.count()).andReturn(1);
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);
      
      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertFalse("With empty or missing property the index should not work.",ie.isProperlyConfigured());
   }
   
   @Test public void isProperlyConfiguredSingleValuePropertyNames(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.count()).andReturn(1);
      expect(names.getValue(Type.NAME,0)).andReturn("jcr:lastModified").anyTimes();
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);
      
      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertNotNull("With a correct property set 'propertyNames' can't be null",ie.getPropertyNames());
      assertEquals(1,ie.getPropertyNames().size());
      assertEquals("jcr:lastModified",ie.getPropertyNames().iterator().next());
      assertTrue("Expecting a properly configured index",ie.isProperlyConfigured());
   }
   
   @Test public void multiValueProperty(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.isArray()).andReturn(true).anyTimes();
      expect(names.count()).andReturn(2).anyTimes();
      expect(names.getValue(Type.NAME,0)).andReturn("jcr:lastModified").anyTimes();
      expect(names.getValue(Type.NAME,1)).andReturn("foo:bar").anyTimes();
      expect(names.getValue(Type.NAMES)).andReturn(Arrays.asList("jcr:lastModified","foo:bar")).anyTimes();
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);

      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertNotNull("With a correct property set 'propertyNames' can't be null",ie.getPropertyNames());
      assertEquals("When multiple properties are a passed only the first one is taken", 1,ie.getPropertyNames().size());
      assertEquals("jcr:lastModified",ie.getPropertyNames().iterator().next());
      assertTrue("Expecting a properly configured index",ie.isProperlyConfigured());
   }
   
   @Test
   public void orderedDirectionFromString(){
       assertNull("A non-existing order direction should result in null",OrderDirection.fromString("foobar"));
       assertEquals(OrderDirection.ASC, OrderDirection.fromString("ascending"));
       assertFalse(OrderDirection.ASC.equals(OrderDirection.fromString("descending")));
       assertEquals(OrderDirection.DESC,OrderDirection.fromString("descending"));
       assertFalse(OrderDirection.DESC.equals(OrderDirection.fromString("ascending")));
   }
   
   @Test
   public void orderDirectionDefinitionNotSpecified(){
       final String property = "foobar";
       NodeBuilder definition = EmptyNodeState.EMPTY_NODE.builder();
       definition.setProperty(IndexConstants.PROPERTY_NAMES, property);
       OrderedPropertyIndexEditor editor = new OrderedPropertyIndexEditor(definition, null, null);
       assertNotNull(editor.getPropertyNames());
       assertEquals(property,editor.getPropertyNames().iterator().next());
       assertEquals(OrderedIndex.OrderDirection.ASC,editor.getDirection());
   }

   @Test 
   public void orderDirectionDefinitionDescending(){
       final String property = "foobar";
       NodeBuilder definition = EmptyNodeState.EMPTY_NODE.builder();
       definition.setProperty(IndexConstants.PROPERTY_NAMES, property);
       definition.setProperty(OrderedIndex.DIRECTION,"descending");
       OrderedPropertyIndexEditor editor = new OrderedPropertyIndexEditor(definition, null, null);
       assertNotNull(editor.getPropertyNames());
       assertEquals(property,editor.getPropertyNames().iterator().next());
       assertEquals(OrderedIndex.OrderDirection.DESC,editor.getDirection());
   }
   
   @Test
   public void orderDirectionUnknownDefinition(){
       final String property = "foobar";
       NodeBuilder definition = EmptyNodeState.EMPTY_NODE.builder();
       definition.setProperty(IndexConstants.PROPERTY_NAMES, property);
       definition.setProperty(OrderedIndex.DIRECTION,"bazbaz");
       OrderedPropertyIndexEditor editor = new OrderedPropertyIndexEditor(definition, null, null);
       assertNotNull(editor.getPropertyNames());
       assertEquals(property,editor.getPropertyNames().iterator().next());
       assertEquals("if we provide a non-valid definition for order the Ascending is expected",OrderedIndex.OrderDirection.ASC,editor.getDirection());
   }
   
   @Test
   public void strategies(){
       final String property = "foobar";
       NodeBuilder definition = EmptyNodeState.EMPTY_NODE.builder();
       definition.setProperty(IndexConstants.PROPERTY_NAMES, property);
       definition.setProperty(OrderedIndex.DIRECTION,OrderDirection.ASC.getDirection());
       OrderedPropertyIndexEditor editor = new OrderedPropertyIndexEditor(definition, null, null);
       assertEquals(OrderedPropertyIndexEditor.ORDERED_MIRROR,editor.getStrategy(false));
       
       definition = EmptyNodeState.EMPTY_NODE.builder();
       definition.setProperty(IndexConstants.PROPERTY_NAMES, property);
       definition.setProperty(OrderedIndex.DIRECTION,OrderDirection.DESC.getDirection());
       editor = new OrderedPropertyIndexEditor(definition, null, null);
       assertEquals(OrderedPropertyIndexEditor.ORDERED_MIRROR_DESCENDING,editor.getStrategy(false));
   }
}
