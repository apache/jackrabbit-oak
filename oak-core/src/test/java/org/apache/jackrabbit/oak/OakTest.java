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
package org.apache.jackrabbit.oak;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.jcr.NoSuchWorkspaceException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * OakTest... TODO
 */
public class OakTest {

    @Test
    public void testWithDefaultWorkspaceName() throws Exception {
        ContentRepository repo = new Oak().with("test").with(new OpenSecurityProvider()).createContentRepository();

        String[] valid = new String[] {null, "test"};
        for (String wspName : valid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                assertEquals("test", cs.getWorkspaceName());
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

        String[] invalid = new String[] {"", "another", Oak.DEFAULT_WORKSPACE_NAME};
        for (String wspName : invalid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                fail("invalid workspace nam");
            } catch (NoSuchWorkspaceException e) {
                // success
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

    }



    @Test
    public void closeAsyncIndexers() throws Exception{
        final AtomicReference<AsyncIndexUpdate> async = new AtomicReference<AsyncIndexUpdate>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                if (service instanceof AsyncIndexUpdate){
                    async.set((AsyncIndexUpdate) service);
                }
                return super.register(type, service, properties);
            }
        };
        Oak oak = new Oak()
                .with(new OpenSecurityProvider())
                .with(wb)
                .withAsyncIndexing();
        ContentRepository repo = oak.createContentRepository();

        ((Closeable)repo).close();
        assertNotNull(async.get());
        assertTrue(async.get().isClosed());
        assertNull(WhiteboardUtils.getService(wb, AsyncIndexUpdate.class));
    }

}