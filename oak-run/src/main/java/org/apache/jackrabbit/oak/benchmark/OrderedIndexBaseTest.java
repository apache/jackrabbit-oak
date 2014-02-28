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

package org.apache.jackrabbit.oak.benchmark;

import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;

/**
 *
 */
public abstract class OrderedIndexBaseTest extends AbstractTest {
    /**
     * the number of nodes created per iteration
     */
    static final int NODES_PER_ITERATION = Integer.parseInt(System.getProperty("nodesPerIteration", "100"));
    
    /**
     * number of nodes that has to be added before performing the actual test
     */
    static final int PRE_ADDED_NODES = Integer.parseInt(System.getProperty("preAddedNodes", "0"));

    /**
    * type of the created node
    */
   static final String NODE_TYPE = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
      
   /**
    * property that will be indexed
    */
   static final String INDEXED_PROPERTY = "indexedProperty";
   
   /**
    * node name below which creating the test data
    */
   final String DUMP_NODE = this.getClass().getSimpleName() + TEST_ID;

   /**
    * session used for operations throughout the test
    */
   Session session;
   
   /**
    * node under which all the test data will be filled in
    */
   Node dump;
      
   void insertRandomNodes(int numberOfNodes){
      try{
         for(int i=0; i<numberOfNodes; i++){
            String uuid = UUID.randomUUID().toString();
            dump.addNode(uuid, NODE_TYPE).setProperty(INDEXED_PROPERTY, uuid);
            session.save();            
         }
      } catch (RepositoryException e){
         throw new RuntimeException(e);
      }      
   }

   /**
    * override when needed to define an index
    */
   void defineIndex() throws Exception{
   }
   
   Node defineStandardPropertyIndex(Session session) throws Exception {
       Node index = new OakIndexUtils.PropertyIndex().property(INDEXED_PROPERTY).create(session);
       if(index == null) {
           throw new RuntimeException("Error while creating the index definition. index node is null.");
       }
       if(!PropertyIndexEditorProvider.TYPE.equals(index.getProperty(IndexConstants.TYPE_PROPERTY_NAME).getString())) {
           throw new RuntimeException("The type of the index does not match the expected");
       }
       session.save();
       return index;
   }
   
   Node defineOrderedPropertyIndex(Session session) throws Exception {
       Node index = new OakIndexUtils.PropertyIndex().property(INDEXED_PROPERTY).create(session,OrderedPropertyIndexEditorProvider.TYPE);
       if(index == null) {
           throw new RuntimeException("Error while creating the index definition. index node is null.");
       }
       if(!OrderedPropertyIndexEditorProvider.TYPE.equals(index.getProperty(IndexConstants.TYPE_PROPERTY_NAME).getString())) {
           throw new RuntimeException("The index type does not match the expected");
       }
       session.save();
       return index;
   }
}
