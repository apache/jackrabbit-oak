package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.*;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.junit.Assert.*;

public class BranchCommitGCTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private Clock clock;
    private DocumentNodeStore store;
    private VersionGarbageCollector gc;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = builderProvider.newBuilder()
                .clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setAsyncDelay(0)
                .getNodeStore();
        gc = store.getVersionGarbageCollector();
    }

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.dispose();
        }
        Revision.resetClockToDefault();
    }

    @Ignore
    @Test
    public void orphanedBranchCommitDetect() throws Exception {

        NodeBuilder b1 = store.getRoot().builder();
        b1.child("a");
        b1.child("b");
        persistToBranch(b1);

        // b1 must see 'a' and 'b'
        assertTrue(b1.hasChildNode("a"));
        assertTrue(b1.hasChildNode("b"));

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        VersionGarbageCollector.VersionGCStats stats= gc.gc(1, HOURS);

        //This will fail as of now but will pass once BranchCommit GC code is merged.
        assertEquals(1, stats.deletedDocGCCount);
    }

    @Ignore
    @Test
    public void orphanedModifiedBranchCommitDetect() throws Exception {

        NodeBuilder b = store.getRoot().builder();
        b.child("foo");
        b.child("test");
        persistToBranch(b);

        // b must see 'a' and 'b'
        assertTrue(b.hasChildNode("foo"));
        assertTrue(b.hasChildNode("test"));

        merge(b);

        b = store.getRoot().builder();
        b.child("test").remove();
        b.getChildNode("foo").child("childfoo");
        persistToBranch(b);
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        VersionGarbageCollector.VersionGCStats stats= gc.gc(1, HOURS);

        //This will fail as of now but will pass once BranchCommit GC code is merged.
        assertEquals(1, stats.deletedDocGCCount);
    }

    private void merge(NodeBuilder builder)
            throws Exception {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
