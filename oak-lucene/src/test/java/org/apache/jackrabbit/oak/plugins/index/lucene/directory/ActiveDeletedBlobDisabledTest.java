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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executor;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.lucene.ActiveDeletedBlobCollectorMBeanImpl;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.junit.Test;

public class ActiveDeletedBlobDisabledTest {


    private void test(final boolean disabled) {
        String property = "oak.active.deletion.disabled";
        try {
            System.setProperty(property, "" + disabled);
            NodeStore store = null;
            IndexPathService indexPathService = null;
            AsyncIndexInfoService asyncIndexInfoService = null;
            Executor executor = new Executor() {
                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };
            ActiveDeletedBlobCollectorMBeanImpl b = new ActiveDeletedBlobCollectorMBeanImpl(
                    ActiveDeletedBlobCollectorFactory.NOOP,
                    new DefaultWhiteboard(),
                    store,
                    indexPathService,
                    asyncIndexInfoService,
                    new MemoryBlobStore(),
                    executor);
            b.isActiveDeletionUnsafe();
            b.isDisabled();
            CompositeData d = b.startActiveCollection();
            if (disabled) {
                // none
                assertEquals(1, d.get("code"));
                assertEquals("Active deletion is disabled", d.get("message"));
            } else {
                // failed
                assertEquals(5, d.get("code"));
                assertEquals("Active lucene index blobs collection couldn't be run as a safe timestamp for purging lucene index blobs couldn't be evaluated",
                        d.get("message"));
            }
        } finally {
            System.clearProperty(property);
        }

    }

    @Test
    public void testDisabled() {
        test(true);
    }

    @Test
    public void testEnabled() {
        test(false);
    }

}
