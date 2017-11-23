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

import java.util.List;

import org.junit.rules.ExternalResource;

import com.google.common.collect.Lists;

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

    private List<DisposingDocumentMKBuilder> builders = Lists.newArrayList();
    
    @Override
    protected void after() {
        for (DisposingDocumentMKBuilder builder : builders) {
            builder.dispose();
        }
    }
    
    public DocumentMK.Builder newBuilder() {
        DisposingDocumentMKBuilder builder = new DisposingDocumentMKBuilder();
        builders.add(builder);
        return builder;
    }
    
    private static class DisposingDocumentMKBuilder extends DocumentMK.Builder {
        
        private boolean initialised = false;
        
        @Override
        public DocumentNodeStore getNodeStore() {
            initialised = true;
            return super.getNodeStore();
        }

        public void dispose() {
            if ( initialised ) {
                getNodeStore().dispose();
            }
        }
    }        
}