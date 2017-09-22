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
package org.apache.jackrabbit.oak.composite;

import com.google.common.io.Closer;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.query.Query;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.junit.Assert.assertEquals;

public class AtomicCompositeMergeTest {

    private static final Logger LOG = LoggerFactory.getLogger(AtomicCompositeMergeTest.class);

    private static final int THREADS = 6;

    private static final String TEST_UUID = UUIDUtils.generateUUID();

    private Closer closer;

    @Before
    public void initNodeStore() {
        closer = Closer.create();
    }

    @After
    public void closeAll() throws IOException {
        closer.close();
    }

    @Test
    public void testLocalMerges() throws InterruptedException, IOException, ParseException, CommitFailedException {
        Oak oak = getOak(getCompositeNodeStore(new MemoryNodeStore()));
        testAtomicMerges(clusterId -> oak);
    }

    @Test
    public void testDistributedMerge() throws InterruptedException, IOException, ParseException, CommitFailedException {
        MemoryDocumentStore sharedDocStore = new MemoryDocumentStore();
        testAtomicMerges(clusterId -> {
            DocumentNodeStore docNodeStore = new DocumentMK.Builder()
                    .setDocumentStore(sharedDocStore)
                    .setClusterId(clusterId)
                    .getNodeStore();
            closer.register(() -> docNodeStore.dispose());
            NodeStore compositeNodeStore = getCompositeNodeStore(docNodeStore);
            return getOak(compositeNodeStore);
        });
    }

    private void testAtomicMerges(Function<Integer, Oak> oakSupplier) throws InterruptedException, IOException, ParseException, CommitFailedException {
        Set<String> failedMerges = Collections.synchronizedSet(newHashSet());
        List<Thread> threads = newArrayList();

        ContentSession generalSession = oakSupplier.apply(100).createContentSession();
        closer.register(generalSession);
        waitForReindexing(generalSession);
        sleep(1000); // sleep for a sec, so the new repository have chance to initialize itself

        for (int i = 0; i < THREADS; i++) {
            String name = "child_" + i;
            Oak oak = oakSupplier.apply(i + 1);
            ContentSession session = oak.createContentSession();
            threads.add(new Thread(() -> {
                LOG.info("Started thread {}", name);
                try {
                    Root root = session.getLatestRoot();
                    root.getTree("/").addChild(name).setProperty(JcrConstants.JCR_UUID, TEST_UUID);
                    root.commit();
                    LOG.info("Merged successfully the node /{}: {}", name, root.getTree("/" + name));
                } catch (CommitFailedException e) {
                    LOG.info("Expected failure", e);
                    failedMerges.add(name);
                } catch (Exception e) {
                    LOG.error("Can't commit", e);
                } finally {
                    IOUtils.closeQuietly(session);
                }
            }));
        }

        threads.forEach(Thread::start);
        threads.forEach(AtomicCompositeMergeTest::join);

        List<String> uuidPaths = waitForUuid(generalSession, TEST_UUID);

        assertEquals("There should be just one indexed value for the TEST_UUID, but following are given: " + uuidPaths + ". Failed merge list: " + failedMerges, 1, uuidPaths.size());
        assertEquals("There should be " + (THREADS - 1) + " failed merges, but following are given: " + failedMerges,THREADS - 1, failedMerges.size());
    }

    private static List<String> waitForUuid(ContentSession session, String uuid) throws ParseException {
        for (int i = 0; i < 20; i++) {
            List<String> result = queryUuid(session, uuid);
            if (result.isEmpty()) {
                sleep(500);
            } else {
                return result;
            }
        }
        return Collections.emptyList();
    }

    private static List<String> queryUuid(ContentSession session, String uuid) throws ParseException {
        Map<String, PropertyValue> bindings = Collections.singletonMap("id", PropertyValues.newString(uuid));
        Result result = session.getLatestRoot().getQueryEngine().executeQuery(
                "SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id" + QueryEngine.INTERNAL_SQL2_QUERY,
                Query.JCR_SQL2,
                bindings, NO_MAPPINGS);
        return StreamSupport.stream(result.getRows().spliterator(), false)
                .map(r -> r.getPath())
                .collect(Collectors.toList());
    }

    private void waitForReindexing(ContentSession session) throws CommitFailedException, ParseException {
        String tmpUuid = UUIDUtils.generateUUID();

        Root root = session.getLatestRoot();
        root.getTree("/").addChild("tmp").setProperty(JcrConstants.JCR_UUID, tmpUuid);
        root.commit();

        assertEquals(1, waitForUuid(session, tmpUuid).size());

        root.getTree("/tmp").remove();
        root.commit();
    }

    private static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.error("Interrupted", e);
        }
    }

    private static NodeStore getCompositeNodeStore(NodeStore globalNodeStore) {
        return new CompositeNodeStore(Mounts.defaultMountInfoProvider(), globalNodeStore, Collections.emptyList());
    }

    private static Oak getOak(NodeStore nodeStore) {
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new InitialContent());

    }
}
