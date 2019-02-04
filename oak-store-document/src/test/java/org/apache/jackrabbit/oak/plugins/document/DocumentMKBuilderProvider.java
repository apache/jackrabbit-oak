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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <tt>DocumentMkBuilderProvider</tt> is a JUnit <tt>@Rule</tt> which
 * automatically disposes created <tt>DocumentNodeStore</tt> instances
 * 
 * <p>Usage samples are below</p>
 * 
 *  <p>Before:</p>
 *  
 *  <pre>
 *  @Test public void someTest() {
 *      DocumentNodeStore = new DocumentMK.Builder().getNodeStore();
 *  }</pre>
 *  
 *  <p>After:</p>
 *  
 *  <pre>
 *  @Rule
    public DocumentMkBuilderProvider builderProvider = new DocumentMkBuilderProvider();
 *  
 *  @Test public void someTest() {
 *      DocumentNodeStore = builderProvider.newBuilder().getNodeStore();
 *  }</pre>
 *
 */
public class DocumentMKBuilderProvider extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentMKBuilderProvider.class);

    private List<DocumentNodeStore> nodeStores = new ArrayList<>();
    
    @Override
    protected void after() {
        for (DocumentNodeStore ns : nodeStores) {
            try {
                ns.dispose();
            } catch (Exception e) {
                LOG.warn("Exception while disposing DocumentNodeStore", e);
            }
        }
    }

    public DocumentMK.Builder newBuilder() {
        return new DisposingDocumentMKBuilder();
    }
    
    private class DisposingDocumentMKBuilder extends DocumentMK.Builder {
        
        @Override
        public DocumentNodeStore getNodeStore() {
            DocumentNodeStore ns = super.getNodeStore();
            nodeStores.add(ns);
            return ns;
        }

        @Override
        public DocumentNodeStore build() {
            DocumentNodeStore ns = super.build();
            nodeStores.add(ns);
            return ns;
        }
    }
}