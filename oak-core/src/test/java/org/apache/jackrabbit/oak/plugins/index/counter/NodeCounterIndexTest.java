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
package org.apache.jackrabbit.oak.plugins.index.counter;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;

/**
 * A test case for the node counter index.
 */
public class NodeCounterIndexTest {

    Whiteboard wb;
    NodeStore nodeStore;
    Root root;
    QueryEngine qe;
    ContentSession session;
    
    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }
    
    @Test
    public void testNotUsedBeforeValid() throws Exception {
        root.getTree("/oak:index/counter").setProperty("resolution", 100);
        root.commit();
        // no index data before indexing
        assertFalse(nodeExists("oak:index/counter/:index"));
        // so, cost for traversal is high
        assertTrue(getCost("/jcr:root//*") >= 1.0E8);
        
        runAsyncIndex();
        // sometimes, the :index node doesn't exist because there are very few
        // nodes (randomly, because the seed value of the node counter is random
        // by design) - so we create nodes until the index exists
        // (we could use a fixed seed to ensure this is not the case,
        // but creating nodes has the same effect)
        for(int i=0; !nodeExists("oak:index/counter/:index"); i++) {
            assertTrue("index not ready after 100 iterations", i < 100);
            Tree t = root.getTree("/").addChild("test" + i);
            for (int j = 0; j < 100; j++) {
                t.addChild("n" + j);
            }
            root.commit();
            runAsyncIndex();
        }

        // because we do have node counter data,
        // cost for traversal is low
        assertTrue(getCost("/jcr:root//*") < 1.0E8);

        // remove the counter index
        root.getTree("/oak:index/counter").remove();
        root.commit();
        assertFalse(nodeExists("oak:index/counter"));
        // so, cost for traversal is high again
        assertTrue(getCost("/jcr:root//*") >= 1.0E8);
    }
    
    private double getCost(String xpath) throws ParseException {
        String plan = executeXPathQuery("explain measure " + xpath);
        String cost = plan.substring(plan.lastIndexOf('{'));
        JsonObject json = parseJson(cost);
        double c = Double.parseDouble(json.getProperties().get("a"));
        return c;
    }
    
    private static JsonObject parseJson(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        return JsonObject.create(t);
    }
    
    private boolean nodeExists(String path) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }
    
    protected String executeXPathQuery(String statement) throws ParseException {
        Result result = qe.executeQuery(statement, "xpath", null, NO_MAPPINGS);
        StringBuilder buff = new StringBuilder();
        for (ResultRow row : result.getRows()) {
            for(PropertyValue v : row.getValues()) {
                buff.append(v);
            }
        }
        return buff.toString();
    }
    
    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }
    
    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
        root.refresh();
    }

}
