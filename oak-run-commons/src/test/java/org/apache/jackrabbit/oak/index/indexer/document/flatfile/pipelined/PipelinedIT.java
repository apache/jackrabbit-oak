package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import java.util.function.Predicate;

public class PipelinedIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedIT.class);

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Rule
    public final TestRule restoreSystemProperties = new RestoreSystemProperties();

    @Rule
    public TemporaryFolder sortFolder = new TemporaryFolder();
    private final PathElementComparator pathComparator = new PathElementComparator(Set.of());
    private final Compression compressionAlgorithm = Compression.NONE;

    private final MemoryBlobStore memoryBlobStore = new MemoryBlobStore();

    private final Set<String> preferredPathElements = Set.of();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    DocumentNodeStore dns;

    @Before
    public void setup() throws IOException {
        LOG.info("Running test: setup");
//        try {
//            System.setProperty("java.io.tmpdir", temporaryFolder.newFolder("systemp").getAbsolutePath());
//        } catch (IOException e) {
//            throw e;
//        }
    }

    @After
    public void tear() {
        LOG.info("Running test: tear");
//        if (dns != null) {
//            dns.dispose();
//        }
    }

    private ImmutablePair<MongoDocumentStore, DocumentNodeStore> createNodeStore(boolean readOnly) {
        MongoConnection c = connectionFactory.getConnection();
        DocumentMK.Builder builder = builderProvider.newBuilder();
        builder.setMongoDB(c.getMongoClient(), c.getDBName());
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        return new ImmutablePair<>((MongoDocumentStore) builder.getDocumentStore(), builder.getNodeStore());
    }

    @Test
    public void createFlatFileStore() throws Exception {
        LOG.info("Running test: createFlatFileStore. Mongo: {} {}", MongoUtils.URL, MongoUtils.isAvailable());

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> writeable = createNodeStore(false);
        NodeStore rwNodeStore = writeable.right;
        @NotNull NodeBuilder rootBuilder = rwNodeStore.getRoot().builder();
        @NotNull NodeBuilder contentDamBuilder = rootBuilder.child("content").child("dam");
        contentDamBuilder.child("a").setProperty("foo", "bar");
        rwNodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ImmutablePair<MongoDocumentStore, DocumentNodeStore> readOnly = createNodeStore(true);
        DocumentNodeStore readOnlyNodeStore = readOnly.right;
        MongoDocumentStore readOnlyMongoDocStore = readOnly.left;

        Predicate<String> pathPredicate = s -> s.startsWith("/content/dam");
        RevisionVector rootRevision = readOnlyNodeStore.getRoot().getRootRevision();
        PipelinedStrategy pipelinedStrategy = new PipelinedStrategy(
                readOnlyMongoDocStore,
                readOnlyNodeStore,
                rootRevision,
                preferredPathElements,
                memoryBlobStore,
                sortFolder.getRoot(),
                compressionAlgorithm,
                pathPredicate
        );

        File file = pipelinedStrategy.createSortedStoreFile();
        LOG.info("Created file: {}", file.getAbsolutePath());
        LOG.info("Contents:\n{}", Files.readString(file.toPath()));
    }
}