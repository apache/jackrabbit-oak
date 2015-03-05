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

import java.util.HashMap;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LuceneIndexProviderServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private LuceneIndexProviderService service = new LuceneIndexProviderService();

    @Test
    public void defaultSetup() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), new HashMap<String, Object>());

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(Observer.class));

        assertTrue(context.getService(Observer.class) instanceof BackgroundObserver);

        MockOsgi.deactivate(service);
    }

    @Test
    public void disableOpenIndexAsync() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), ImmutableMap.<String,Object>of("enableOpenIndexAsync", false));

        assertTrue(context.getService(Observer.class) instanceof LuceneIndexProvider);

        MockOsgi.deactivate(service);
    }
}
