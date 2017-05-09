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
package org.apache.jackrabbit.oak.plugins.document;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;

import junitx.util.PrivateAccessor;

/**
 * Tests for the DocumentDiscoveryLiteService
 */
public class DocumentDiscoveryLiteServiceTest extends BaseDocumentDiscoveryLiteServiceTest {

    @Test
    public void testActivateDeactivate() throws Exception {
        // then test normal start with a DocumentNodeStore
        DocumentMK mk1 = createMK(1, 0);
        DocumentDiscoveryLiteService discoveryLite = new DocumentDiscoveryLiteService();
        PrivateAccessor.setField(discoveryLite, "nodeStore", mk1.nodeStore);
        BundleContext bc = mock(BundleContext.class);
        ComponentContext c = mock(ComponentContext.class);
        when(c.getBundleContext()).thenReturn(bc);
        discoveryLite.activate(c);
        verify(c, times(0)).disableComponent(DocumentDiscoveryLiteService.COMPONENT_NAME);
        discoveryLite.deactivate();
    }

    @Test
    public void testOneNode() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final ViewExpectation expectation = new ViewExpectation(s1);
        expectation.setActiveIds(s1.ns.getClusterId());
        waitFor(expectation, 2000, "see myself as active");
    }

    @Test
    public void testTwoNodesWithCleanShutdown() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.shutdown();
        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 2000, "first should only see itself after shutdown");
    }

    @Test
    public void testTwoNodesWithCrash() throws Throwable {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.crash();

        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 4000, "first should only see itself after shutdown");
    }

    /**
     * This test creates a large number of documentnodestores which it starts,
     * runs, stops in a random fashion, always testing to make sure the
     * clusterView is correct
     */
    @Test
    public void testSmallStartStopFiesta() throws Throwable {
        logger.info("testSmallStartStopFiesta: start, seed="+SEED);
        final int LOOP_CNT = 5; // with too many loops have also seen mongo
                                 // connections becoming starved thus test
                                 // failed
        doStartStopFiesta(LOOP_CNT);
    }

}
