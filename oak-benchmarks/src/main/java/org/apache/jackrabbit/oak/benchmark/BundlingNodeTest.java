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

package org.apache.jackrabbit.oak.benchmark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;

import com.google.common.base.Joiner;
import com.google.common.collect.EvictingQueue;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.commons.JcrUtils.getOrAddNode;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_RESOURCE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;

public class BundlingNodeTest extends AbstractTest<BundlingNodeTest.TestContext> {
    enum Status {
        NONE, STARTING, STARTED, STOPPING, STOPPED, ABORTED;

        private int count;

        public void inc(){
            count++;
        }

        public int count(){
            return count;
        }

        public Status next(){
            Status[] ss = values();
            if (ordinal() == ss.length - 1){
                return ss[0];
            }
            return ss[ordinal() + 1];
        }
    }

    private enum BundlingMode {ALL, EXCLUDE_RENDITIONS}

    private static final String NT_OAK_ASSET = "oak:Asset";

    private static final String ASSET_NODE_TYPE_DEFN = "[oak:Asset]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:Asset VERSION";

    private final Random random = new Random(42); //fixed seed
    private int nodesPerFolder = 10;

    private boolean bundlingEnabled = Boolean.parseBoolean(System.getProperty("bundlingEnabled", "false"));
    private boolean oakResourceEnabled = Boolean.parseBoolean(System.getProperty("oakResourceEnabled", "false"));
    private boolean readerEnabled = Boolean.parseBoolean(System.getProperty("readerEnabled", "true"));
    private BundlingMode bundlingMode = BundlingMode.valueOf(System.getProperty("bundlingMode", "all").toUpperCase());
    private RepositoryInitializer bundlingInitializer = new BundlingConfigInitializer();
    private TestContext defaultContext;
    private Reader reader;
    private Mutator mutator;
    private final AtomicInteger assetCount = new AtomicInteger();
    private List<TestContext> contexts = new ArrayList<>();
    private String contentNodeType = NT_RESOURCE;

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    Jcr jcr = new Jcr(oak);
                    if (bundlingEnabled) {
                        jcr.with(bundlingInitializer);
                    }
                    jcr.with(FixNodeTypeIndexInitializer.INSTANCE);
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    public void beforeSuite() throws Exception {
        if (oakResourceEnabled) {
            contentNodeType = NT_OAK_RESOURCE;
        }
        registerAssetNodeType();
        defaultContext = new TestContext();
        contexts.add(defaultContext);
        reader = new Reader();
        mutator = new Mutator();

        if (readerEnabled) {
            addBackgroundJob(reader);
        }
        addBackgroundJob(mutator);
    }

    @Override
    protected TestContext prepareThreadExecutionContext() throws RepositoryException {
        TestContext ctx = new TestContext();
        contexts.add(ctx);
        return ctx;
    }

    @Override
    protected void runTest() throws Exception {
        runTest(defaultContext);
    }

    @Override
    protected void runTest(TestContext ctx)  throws Exception {
        //Create tree in breadth first fashion with each node having 50 child
        Node parent = ctx.session.getNode(ctx.paths.remove());
        Status status = Status.NONE;
        parent = parent.addNode(nextNodeName(), NT_OAK_UNSTRUCTURED);
        for (int i = 0; i < nodesPerFolder; i++) {
            Node asset = parent.addNode(nextNodeName()+".png", NT_OAK_ASSET);

            createAssetNodeStructure(asset, status.name());

            ctx.session.save();
            ctx.addAssetPath(asset.getPath());
            assetCount.incrementAndGet();
            status.inc();
            status = status.next();
        }

        ctx.addFolderPath(parent.getPath());
    }

    private void createAssetNodeStructure(Node asset, String status) throws RepositoryException {
        Node content = asset.addNode("jcr:content", NT_OAK_UNSTRUCTURED);
        Node metadata = content.addNode("metadata", NT_OAK_UNSTRUCTURED);
        metadata.setProperty("status", status);

        Node renditions = content.addNode("renditions", NT_FOLDER);
        for (int i = 0; i < 5; i++) {
            addFile(renditions, "thumbnail-" + i + ".png");
        }

        content.addNode("comments", NT_OAK_UNSTRUCTURED);
    }

    private void addFile(Node parent, String fileName) throws RepositoryException {
        Node file = getOrAddNode(parent, fileName, NodeType.NT_FILE);
        Node content = getOrAddNode(file, Node.JCR_CONTENT, contentNodeType);
        content.setProperty(Property.JCR_MIMETYPE, "text/plain");
        content.setProperty(Property.JCR_LAST_MODIFIED, Calendar.getInstance());

        Binary binary = parent.getSession().getValueFactory().createBinary(new ByteArrayInputStream("hello".getBytes()));
        content.setProperty(Property.JCR_DATA, binary);
        binary.dispose();
    }

    @Override
    protected void disposeThreadExecutionContext(TestContext context) throws RepositoryException {
        context.dispose();
    }

    @Override
    protected void afterSuite() throws Exception {
        System.out.printf("bundlingEnabled: %s, oakResourceEnabled: %s, readerEnabled: %s, bundlingMode: %s%n",
                bundlingEnabled, oakResourceEnabled, readerEnabled, bundlingMode);
    }

    @Override
    protected String[] statsNames() {
        return new String[]{"Reader", "Mutator", "Assets#"};
    }

    @Override
    protected String[] statsFormats() {
        return new String[]{"%8d", "%6d", "%6d"};
    }

    @Override
    protected Object[] statsValues() {
        return new Object[]{reader.readCount, mutator.mutationCount, assetCount.get()};
    }

    @Override
    protected String comment() {
        List<String> commentElements = new ArrayList<>();
        if (bundlingEnabled){
            commentElements.add("bundling");
        }
        commentElements.add(contentNodeType);
        commentElements.add(bundlingMode.name());
        return Joiner.on(',').join(commentElements);
    }

    protected class TestContext {
        final Session session = loginWriter();
        final Queue<String> paths = new LinkedBlockingDeque<>();
        final int assetSampleSize = 50;
        final EvictingQueue<String> buffer = EvictingQueue.create(assetSampleSize);
        private List<String> assets;
        private int assetCount = 0;


        final Node dump;

        public TestContext() throws RepositoryException {
            dump = session.getRootNode()
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED)
                    .addNode(nextNodeName(), NT_OAK_UNSTRUCTURED);
            session.save();
            paths.add(dump.getPath());
        }

        public void dispose() throws RepositoryException {
            dump.remove();
            session.logout();
        }

        @CheckForNull
        public String pickRandomPath(){
            if (assets != null){
                return assets.get(random.nextInt(assets.size()));
            }
            return buffer.peek();
        }

        public void addAssetPath(String path) {
            buffer.add(path);
            if (++assetCount % assetSampleSize == 0){
                assets = new ArrayList<>(buffer);
            }
        }

        public void addFolderPath(String path) {
            paths.add(path);
        }
    }

    private TestContext randomContext() {
        return contexts.get(random.nextInt(contexts.size()));
    }

    private void registerAssetNodeType() throws ParseException, RepositoryException, IOException {
        Session session = loginWriter();
        CndImporter.registerNodeTypes(new StringReader(ASSET_NODE_TYPE_DEFN), session);
        session.logout();
    }

    private class BundlingConfigInitializer implements RepositoryInitializer {

        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            if (builder.hasChildNode(JCR_SYSTEM)){
                NodeBuilder system = builder.getChildNode(JCR_SYSTEM);

                if (!system.hasChildNode(DOCUMENT_NODE_STORE)){
                    NodeBuilder dns = jcrChild(system, DOCUMENT_NODE_STORE);
                    NodeBuilder bundlor = jcrChild(dns, BUNDLOR);

                    addPattern(bundlor, "nt:file", "jcr:content");

                    switch (bundlingMode) {
                        case ALL :
                            addPattern(bundlor, "oak:Asset", "jcr:content",
                                    "jcr:content/metadata",
                                    "jcr:content/renditions",
                                    "jcr:content/renditions/**"
                            );
                            break;
                        case EXCLUDE_RENDITIONS:
                            addPattern(bundlor, "oak:Asset", "jcr:content",
                                    "jcr:content/metadata"
                            );
                            break;
                    }
                }
            }
        }

        private void addPattern(NodeBuilder builder, String type, String ... patterns){
            NodeBuilder child = jcrChild(builder, type);
            child.setProperty(createProperty("pattern", Arrays.asList(patterns), STRINGS));
        }

        private NodeBuilder jcrChild(NodeBuilder builder, String name){
            NodeBuilder child = builder.child(name);
            child.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
            return child;
        }
    }

    private enum FixNodeTypeIndexInitializer implements RepositoryInitializer {
        INSTANCE;

        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            NodeBuilder nodetype = builder.getChildNode("oak:index").getChildNode("nodetype");
            if (nodetype.exists()){
                nodetype.setProperty(DECLARING_NODE_TYPES, Collections.singleton("rep:Authorizable"), Type.NAMES);
            }
        }
    }

    private class Reader implements Runnable {
        final Session session = loginWriter();
        int readCount = 0;
        @Override
        public void run() {
            try{
                run0();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void run0() throws Exception {
            for (int i = 0; i < 25; i++) {
                String path = randomContext().pickRandomPath();
                if (path != null) {
                    session.refresh(false);
                    Node asset = session.getNode(path);
                    asset.getProperty("jcr:content/metadata/status");
                    readCount++;
                }
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private class Mutator implements Runnable {
        final Session session = loginWriter();
        int mutationCount = 0;
        @Override
        public void run() {
            try{
                run0();
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

        private void run0() throws RepositoryException {
            TestContext ctx = randomContext();
            String path = ctx.pickRandomPath();
            if (path != null){
                session.refresh(false);
                Node asset = session.getNode(path);
                Node metadata = asset.getNode("jcr:content/metadata");
                String status = metadata.getProperty("status").getString();
                metadata.setProperty("status", Status.valueOf(status).next().name());
                session.save();
                mutationCount++;
            }
        }
    }
}
