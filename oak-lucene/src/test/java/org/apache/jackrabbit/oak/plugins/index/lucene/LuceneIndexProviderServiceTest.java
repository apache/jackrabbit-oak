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
import java.util.Map;

import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LuceneIndexProviderServiceTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final OsgiContext context = new OsgiContext();

    private LuceneIndexProviderService service = new LuceneIndexProviderService();

    @Test
    public void defaultSetup() throws Exception{
        MockOsgi.activate(service, context.bundleContext(), getDefaultConfig());

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(Observer.class));

        assertNotNull("CopyOnRead should be enabled by default",context.getService(CopyOnReadStatsMBean.class));

        assertTrue(context.getService(Observer.class) instanceof BackgroundObserver);

        MockOsgi.deactivate(service);
    }

    @Test
    public void disableOpenIndexAsync() throws Exception{
        Map<String,Object> config = getDefaultConfig();
        config.put("enableOpenIndexAsync", false);
        MockOsgi.activate(service, context.bundleContext(), config);

        assertTrue(context.getService(Observer.class) instanceof LuceneIndexProvider);

        MockOsgi.deactivate(service);
    }

    private Map<String,Object> getDefaultConfig(){
        Map<String,Object> config = new HashMap<String, Object>();
        config.put("localIndexDir", folder.getRoot().getAbsolutePath());
        return config;
    }
}
