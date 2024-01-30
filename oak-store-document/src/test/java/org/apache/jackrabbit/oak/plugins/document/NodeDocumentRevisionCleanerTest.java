package org.apache.jackrabbit.oak.plugins.document;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NodeDocumentRevisionCleanerTest {

    @Mock
    DocumentStore documentStore;

    @Mock
    DocumentNodeStore documentNodeStore;

    @Mock
    Checkpoints checkpoints;

    //@Mock
    NodeDocument workingDocument;

    NodeDocumentRevisionCleaner NodeDocumentRevisionCleaner;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        //workingDocument = Mockito.mock(NodeDocument.class);

        /*documentStore = new MemoryDocumentStore();
        documentNodeStore = new DocumentMK.Builder()
                .setDocumentStore(documentStore).setAsyncDelay(0).setClusterId(1).build();*/

        workingDocument = new NodeDocument(documentStore);

        Mockito.when(documentStore.find(Mockito.eq(NODES), Mockito.anyString())).thenReturn(workingDocument);
        Mockito.when(documentNodeStore.getBranches()).thenReturn(new UnmergedBranches());
        NodeDocumentRevisionCleaner = new NodeDocumentRevisionCleaner(documentStore, documentNodeStore, "/");
    }

    @Test
    public void testMarkCheckpointRevisionsToPreserveOnePropertyOneCluster() throws IOException {
        Revision revisionA = Revision.fromString("r100000000-0-1");
        Revision revisionB = Revision.fromString("r105000000-0-1");
        Revision revisionC = Revision.fromString("r110000000-0-1");
        Revision revisionD = Revision.fromString("r115000000-0-1");
        Revision revisionE = Revision.fromString("r120000000-0-1");
        Revision revisionF = Revision.fromString("r125000000-0-1");

        Revision checkpoint1 = Revision.fromString("r109000000-0-1");
        Revision checkpoint2 = Revision.fromString("r119000000-0-1");

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3', '" + revisionD + "': 'value4', '" + revisionE + "': 'value5', '" + revisionF + "': 'value6'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c', '" + revisionC + "': 'c', '" + revisionD + "': 'c', '" + revisionE + "': 'c', '" + revisionF + "': 'c'}" +
                "}";
        String jsonCheckpoints = "{" +
                "'" + checkpoint1 + "': {'expires':'200000000','rv':'r109000000-0-1'}," +
                "'" + checkpoint2 + "': {'expires':'200000000','rv':'r119000000-0-1'}" +
                "}";


        prepareDocumentMock(jsonProperties);
        prepareCheckpointsMock(jsonCheckpoints);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();
        NodeDocumentRevisionCleaner.revisionCleaner.markCheckpointRevisionsToPreserve();
        NodeDocumentRevisionCleaner.revisionCleaner.removeCandidatesInList();

        // The revisions blocked should be:
        //  - r105000000-0-1 (blocked by checkpoint r109000000-0-1)
        //  - r115000000-0-1 (blocked by checkpoint r119000000-0-1)
        assertEquals(Set.of(revisionB, revisionD), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revisionA, revisionC, revisionE, revisionF), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
    }

    @Test
    public void testRecentRevisionsArePreserved() throws IOException {
        StringBuilder jsonPropBuilder = new StringBuilder("'prop1': {");
        StringBuilder jsonRevisionsBuilder = new StringBuilder("'_revisions': {");

        Instant currentTime = Instant.now();
        List<Revision> revisions = new ArrayList<>();
        for (int i = 29; i >= 0; i--) {
            long timestamp = currentTime.minus(i, ChronoUnit.HOURS).toEpochMilli();
            Revision revision = new Revision(timestamp, 0, 1);
            revisions.add(revision);

            jsonPropBuilder.append("'").append(revision).append("': 'value").append(i).append("', ");
            jsonRevisionsBuilder.append("'").append(revision).append("': 'c', ");
        }

        // 23 hours and 59 minutes ago -> Should be preserved
        long timestamp = currentTime.minus(23, ChronoUnit.HOURS).minus(59, ChronoUnit.MINUTES).toEpochMilli();
        Revision revision = new Revision(timestamp, 0, 1);
        revisions.add(revision);  // revisions[30]
        jsonPropBuilder.append("'").append(revision).append("': 'value").append(30).append("', ");
        jsonRevisionsBuilder.append("'").append(revision).append("': 'c', ");

        // 24 hours and 1 minute ago -> Should be candidate to clean
        timestamp = currentTime.minus(24, ChronoUnit.HOURS).minus(1, ChronoUnit.MINUTES).toEpochMilli();
        revision = new Revision(timestamp, 0, 1);
        revisions.add(revision);  // revisions[31]
        jsonPropBuilder.append("'").append(revision).append("': 'value").append(30).append("'");
        jsonRevisionsBuilder.append("'").append(revision).append("': 'c'");

        jsonPropBuilder.append("}");
        jsonRevisionsBuilder.append("}");
        StringBuilder jsonBuilder = new StringBuilder("{");
        jsonBuilder.append(jsonPropBuilder).append(", ").append(jsonRevisionsBuilder).append("}");

        // revisions.forEach(rev -> System.out.println(rev.toReadableString()));

        prepareDocumentMock(jsonBuilder.toString());

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();
        NodeDocumentRevisionCleaner.revisionCleaner.markRevisionsNewerThanThresholdToPreserve(24, ChronoUnit.HOURS);
        NodeDocumentRevisionCleaner.revisionCleaner.removeCandidatesInList();

        // The candidate revisions should be the 6 oldest ones (current time -29, -28, -27, -26, -25, -24)
        // and the one created 24 hours and 1 minute ago
        Set<Revision> expectedCandidateRevisions = Stream.concat(
                revisions.subList(0, 6).stream(), Stream.of(revisions.get(31))
        ).collect(Collectors.toSet());

        // Blocked revisions are all the 24 revisions created in the last 24 hours, and the one created 23 hours and 59 minutes ago
        Set<Revision> expectedBlockedRevisions = revisions.subList(6, 31).stream().collect(Collectors.toSet());

        assertEquals(expectedCandidateRevisions, NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
        assertEquals(expectedBlockedRevisions, NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
    }

    @Test
    public void testInitializeCleanupProcessMultipleClusters() throws IOException {
        List<Revision> revs = new ArrayList<>();

        // Some initial revisions (0-2)
        revs.add(Revision.fromString("r100000000-0-1"));
        revs.add(Revision.fromString("r100000001-0-2"));
        revs.add(Revision.fromString("r100000002-0-3"));

        // Blocked by first checkpoint r109000000 (3-5)
        revs.add(Revision.fromString("r106000003-0-1"));
        revs.add(Revision.fromString("r107000004-0-2"));
        revs.add(Revision.fromString("r108000005-0-3"));

        // Some unblocked revisions in middle (6-10)
        revs.add(Revision.fromString("r110000006-0-3"));
        revs.add(Revision.fromString("r110000006-1-3"));
        revs.add(Revision.fromString("r114000007-0-2"));
        revs.add(Revision.fromString("r115000008-0-1"));
        revs.add(Revision.fromString("r118000009-0-1"));

        // Blocked by second checkpoint r118000000 (11-13)
        revs.add(Revision.fromString("r118000009-1-1"));
        revs.add(Revision.fromString("r118000010-0-2"));
        revs.add(Revision.fromString("r118000011-0-3"));

        // Some more unblocked revisions (14-19)
        revs.add(Revision.fromString("r120000012-0-1"));
        revs.add(Revision.fromString("r122000013-0-2"));
        revs.add(Revision.fromString("r122000013-1-2"));
        revs.add(Revision.fromString("r123000014-0-2"));
        revs.add(Revision.fromString("r125000015-0-3"));
        revs.add(Revision.fromString("r130000016-0-1"));

        // Last revision (20-22)
        revs.add(Revision.fromString("r130000016-1-1"));
        revs.add(Revision.fromString("r130000017-0-2"));
        revs.add(Revision.fromString("r130000018-0-3"));

        // Checkpoint revisions
        Revision checkpoint1 = Revision.fromString("r109000000-0-1");
        Revision checkpoint2 = Revision.fromString("r119000000-0-1");

        StringBuilder jsonPropBuilder = new StringBuilder("'prop1': {");
        StringBuilder jsonRevisionsBuilder = new StringBuilder("'_revisions': {");

        for (int i = 0; i < revs.size(); i++) {
            jsonPropBuilder.append("'").append(revs.get(i)).append("': 'value").append(i).append("'");
            jsonRevisionsBuilder.append("'").append(revs.get(i)).append("': 'c'");
            if (i < revs.size() - 1) {
                jsonPropBuilder.append(", ");
                jsonRevisionsBuilder.append(", ");
            }
        }

        jsonPropBuilder.append("}");
        jsonRevisionsBuilder.append("}");
        StringBuilder jsonBuilder = new StringBuilder("{");
        jsonBuilder.append(jsonPropBuilder).append(", ").append(jsonRevisionsBuilder).append("}");

        String jsonCheckpoints = "{" +
                "'" + checkpoint1 + "': {'expires':'200000000','rv':'r106500003-0-1,r107500004-0-2,r108500005-0-3'}," +
                "'" + checkpoint2 + "': {'expires':'200000000','rv':'r118000009-1-1,r118000010-0-2,r118000011-0-3'}" +
                "}";

        prepareDocumentMock(jsonBuilder.toString());
        prepareCheckpointsMock(jsonCheckpoints);

        NodeDocumentRevisionCleaner.initializeCleanupProcess();

        // The revisions blocked should be:
        //  - r106000003-0-3, r118000004-0-2, r108000005-0-3 (referenced by checkpoint 1)
        //  - r118000009-1-1, r118000010-0-2, r118000011-0-3 (referenced by checkpoint 2)
        //  - r130000016-1-1, r130000017-0-2, r130000018-0-3 (last revision)
        assertEquals(Set.of(revs.get(3), revs.get(11), revs.get(20)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revs.get(4), revs.get(12), revs.get(21)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(2));
        assertEquals(Set.of(revs.get(5), revs.get(13), revs.get(22)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(3));

        // Rest of revisions are candidates to clean
        assertEquals(Set.of(revs.get(0), revs.get(9), revs.get(10), revs.get(14), revs.get(19)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
        assertEquals(Set.of(revs.get(1), revs.get(8), revs.get(15), revs.get(16), revs.get(17)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(2));
        assertEquals(Set.of(revs.get(2), revs.get(6), revs.get(7), revs.get(18)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(3));

        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1)));
        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(2), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(2)));
        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(3), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(3)));
    }

    @Test
    public void testCheckpointedRevisionFallback() throws IOException {
        List<Revision> revs = new ArrayList<>();

        revs.add(Revision.fromString("r100000000-0-1"));
        revs.add(Revision.fromString("r100000001-0-2"));
        revs.add(Revision.fromString("r100000002-0-3"));

        revs.add(Revision.fromString("r101100003-0-1"));
        revs.add(Revision.fromString("r101200004-0-2"));
        revs.add(Revision.fromString("r101300005-0-3"));

        revs.add(Revision.fromString("r102000006-0-1"));
        revs.add(Revision.fromString("r102000007-0-3"));

        revs.add(Revision.fromString("r103000008-0-1"));
        revs.add(Revision.fromString("r103000009-0-2"));
        revs.add(Revision.fromString("r103000010-0-3"));

        // Checkpoint
        Revision checkpoint1 = Revision.fromString("r102000000-0-1");

        StringBuilder jsonPropBuilder = new StringBuilder("'prop1': {");
        StringBuilder jsonRevisionsBuilder = new StringBuilder("'_revisions': {");

        for (int i = 0; i < revs.size(); i++) {
            jsonPropBuilder.append("'").append(revs.get(i)).append("': 'value").append(i).append("'");
            jsonRevisionsBuilder.append("'").append(revs.get(i)).append("': 'c'");
            if (i < revs.size() - 1) {
                jsonPropBuilder.append(", ");
                jsonRevisionsBuilder.append(", ");
            }
        }

        jsonPropBuilder.append("}");
        jsonRevisionsBuilder.append("}");
        StringBuilder jsonBuilder = new StringBuilder("{");
        jsonBuilder.append(jsonPropBuilder).append(", ").append(jsonRevisionsBuilder).append("}");

        String jsonCheckpoints = "{" +
                "'" + checkpoint1 + "': {'expires':'200000000','rv':'r102000007-0-1,r102000000-0-2,r102000008-0-3'}" +
                "}";

        prepareDocumentMock(jsonBuilder.toString());
        prepareCheckpointsMock(jsonCheckpoints);

        NodeDocumentRevisionCleaner.initializeCleanupProcess();

        // The revisions blocked should be:
        //  - r103000008-0-1, r103000009-0-2, r103000010-0-3 (last revisions)
        //  - r102000006-0-1, r101300005-0-3, r102000007-0-3 (referenced by checkpoint for clusters 1, 2 and 3 respectively)
        assertEquals(Set.of(revs.get(6), revs.get(8)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revs.get(9)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(2));
        assertEquals(Set.of(revs.get(5), revs.get(7), revs.get(10)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(3));

        // Rest of revisions are candidates to clean
        assertEquals(Set.of(revs.get(0), revs.get(3)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
        assertEquals(Set.of(revs.get(1), revs.get(4)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(2));
        assertEquals(Set.of(revs.get(2)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(3));
    }

    // New test copy
    /*@Test
    public void testInitializeCleanupProcessMultipleClustersNew() throws IOException {
        List<Revision> revs = new ArrayList<>();

        // Some initial revisions (0-2)
        revs.add(Revision.fromString("r100000000-0-1"));
        revs.add(Revision.fromString("r100000001-0-2"));
        revs.add(Revision.fromString("r100000002-0-3"));

        // Blocked by first checkpoint r109000000 (3-5)
        revs.add(Revision.fromString("r106000003-0-1"));
        revs.add(Revision.fromString("r107000004-0-2"));
        revs.add(Revision.fromString("r108000005-0-3"));

        // Some unblocked revisions in middle (6-10)
        revs.add(Revision.fromString("r110000006-0-3"));
        revs.add(Revision.fromString("r110000006-1-3"));
        revs.add(Revision.fromString("r114000007-0-2"));
        revs.add(Revision.fromString("r115000008-0-1"));
        revs.add(Revision.fromString("r118000009-0-1"));

        // Blocked by second checkpoint r118000000 (11-13)
        revs.add(Revision.fromString("r118000009-1-1"));
        revs.add(Revision.fromString("r118000010-0-2"));
        revs.add(Revision.fromString("r118000011-0-3"));

        // Some more unblocked revisions (14-19)
        revs.add(Revision.fromString("r120000012-0-1"));
        revs.add(Revision.fromString("r122000013-0-2"));
        revs.add(Revision.fromString("r122000013-1-2"));
        revs.add(Revision.fromString("r123000014-0-2"));
        revs.add(Revision.fromString("r125000015-0-3"));
        revs.add(Revision.fromString("r130000016-0-1"));

        // Last revision (20-22)
        revs.add(Revision.fromString("r130000016-1-1"));
        revs.add(Revision.fromString("r130000017-0-2"));
        revs.add(Revision.fromString("r130000018-0-3"));

        // Checkpoint revisions
        Revision checkpoint1 = Revision.fromString("r109000000-0-1");
        Revision checkpoint2 = Revision.fromString("r119000000-0-1");

        StringBuilder jsonPropBuilder = new StringBuilder("'prop1': {");
        StringBuilder jsonRevisionsBuilder = new StringBuilder("'_revisions': {");

        for (int i = 0; i < revs.size(); i++) {
            jsonPropBuilder.append("'").append(revs.get(i)).append("': 'value").append(i).append("'");
            jsonRevisionsBuilder.append("'").append(revs.get(i)).append("': 'c'");
            if (i < revs.size() - 1) {
                jsonPropBuilder.append(", ");
                jsonRevisionsBuilder.append(", ");
            }
        }

        jsonPropBuilder.append("}");
        jsonRevisionsBuilder.append("}");
        StringBuilder jsonBuilder = new StringBuilder("{");
        jsonBuilder.append(jsonPropBuilder).append(", ").append(jsonRevisionsBuilder).append(", '_deleted': {'r100000000-0-1': 'false'}").append("}");

        String jsonCheckpoints = "{" +
                "'" + checkpoint1 + "': {'expires':'200000000','rv':'r106500003-0-1,r107500004-0-2,r108500005-0-3'}," +
                "'" + checkpoint2 + "': {'expires':'200000000','rv':'r118000009-1-1,r118000010-0-2,r118000011-0-3'}" +
                "}";

        prepareDocumentMock(jsonBuilder.toString());
        prepareCheckpointsMock(jsonCheckpoints);

        NodeDocumentRevisionCleaner.initializeCleanupProcess();

        RevisionVector readRevision1  = documentNodeStore.getCheckpoints().getCheckpoints().get(checkpoint1).getCheckpoint();
        RevisionVector readRevision2  = documentNodeStore.getCheckpoints().getCheckpoints().get(checkpoint2).getCheckpoint();

        //workingDocument.getLiveRevision();
        DocumentNodeState state1 = workingDocument.getNodeAtRevision(documentNodeStore, readRevision1,null);
        DocumentNodeState state2 = workingDocument.getNodeAtRevision(documentNodeStore, readRevision2,null);

        System.out.println(state1);
        System.out.println(state2);

        // The revisions blocked should be:
        //  - r106000003-0-3, r118000004-0-2, r108000005-0-3 (referenced by checkpoint 1)
        //  - r118000009-1-1, r118000010-0-2, r118000011-0-3 (referenced by checkpoint 2)
        //  - r130000016-1-1, r130000017-0-2, r130000018-0-3 (last revision)
        /*assertEquals(Set.of(revs.get(3), revs.get(11), revs.get(20)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revs.get(4), revs.get(12), revs.get(21)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(2));
        assertEquals(Set.of(revs.get(5), revs.get(13), revs.get(22)), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(3));

        // Rest of revisions are candidates to clean
        assertEquals(Set.of(revs.get(0), revs.get(9), revs.get(10), revs.get(14), revs.get(19)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
        assertEquals(Set.of(revs.get(1), revs.get(8), revs.get(15), revs.get(16), revs.get(17)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(2));
        assertEquals(Set.of(revs.get(2), revs.get(6), revs.get(7), revs.get(18)), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(3));

        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1)));
        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(2), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(2)));
        assertTrue(Collections.disjoint(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(3), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(3)));*/
    //}

    @Test
    public void testLastRevisionIsBlocked() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();
        NodeDocumentRevisionCleaner.revisionCleaner.markLastRevisionForEachProperty();

        assertFalse(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertFalse(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1).contains(revisionB));
        assertEquals(Set.of(revisionC), NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revisionA, revisionB, revisionC), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
    }

    @Test
    public void testFirstDeletedRevisionIsBlocked() throws Exception {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'_deleted': {'" + revisionA + "': 'false', '" + revisionB + "': 'true', '" + revisionC + "': 'false'}," +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        assertTrue(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertEquals(Set.of(revisionA, revisionB, revisionC), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
    }

    @Test
    public void testFirstDeletedRevisionIsBlockedUnordered() throws Exception {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'_deleted': {'" + revisionC + "': 'false', '" + revisionA + "': 'true', '" + revisionB + "': 'false'}," +
                "'_revisions': {'" + revisionC + "': 'c', '" + revisionB + "': 'c', '" + revisionA + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        assertTrue(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertEquals(Set.of(revisionA, revisionB, revisionC), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
    }

    /*@Test
    public void testFirstDeletedRevisionIsBlocked() throws Exception {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'_deleted': {'" + revisionA + "': 'false', '" + revisionB + "': 'true', '" + revisionC + "': 'false'}," +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        //prepareDocumentMock(jsonProperties);

        insertJsonDataToNodeDocument(workingDocument, jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        assertTrue(NodeDocumentRevisionCleaner.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertEquals(Set.of(revisionA, revisionB, revisionC), NodeDocumentRevisionCleaner.getCandidateRevisionsToClean().get(1));
    }*/

    /*private void insertJsonDataToNodeDocument(NodeDocument document, String jsonProperties) throws Exception {
        String json = jsonProperties.replace("'", "\"");

        ObjectMapper mapper = new ObjectMapper();
        // TypeReference helps in specifying the complex type (Map<String, Object>)
        Map<String, Object> parsedData = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

        // Since the data field is a Map<String, Object>, we need to ensure the inner maps are also correctly typed
        for (Map.Entry<String, Object> entry : parsedData.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<?, ?> innerMap = (Map<?, ?>) entry.getValue();
                Map<String, Object> correctedInnerMap = new TreeMap<>();
                for (Map.Entry<?, ?> innerEntry : innerMap.entrySet()) {
                    correctedInnerMap.put(innerEntry.getKey().toString(), innerEntry.getValue());
                }
                parsedData.put(entry.getKey(), correctedInnerMap);
            }
        }

        document.data = parsedData; // Directly setting the protected field
    }*/

    /*@Test
    public void testtest() throws CommitFailedException, NoSuchFieldException, IllegalAccessException {
        DocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = new DocumentMK.Builder().setDocumentStore(store).setAsyncDelay(0).getNodeStore();

        // add properties
        for (int i = 0; i < 10; i++) {
            NodeBuilder nb = ns.getRoot().builder();

            nb.child("x").setProperty("p"+i, i);
            merge(ns, nb);
        }

        NodeDocument nodeDocument = store.find(NODES, "1:/x");
        System.out.println(nodeDocument.getPropertyNames());

        // Mocked data
        Map<String, Object> data = Maps.newHashMap();


        NodeDocument document = new NodeDocument(store, System.currentTimeMillis());
        document.data.put("foo", "bar");
        System.out.println(document.getPropertyNames());

        document.data = Maps.newHashMap();


        System.out.println(document.getPropertyNames());
    }*/

    @Test
    public void testClassifyAndMapRevisionsAndPropertiesWithDeleted() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3'}, " +
                "'prop2': {'" + revisionB + "': 'value4'}, " +
                "'_deleted': {'" + revisionA + "': 'false'}," +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = NodeDocumentRevisionCleaner.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1", "_deleted"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = NodeDocumentRevisionCleaner.getRevisionsModifyingPropertyByCluster();
        assertEquals(Set.of(revisionA, revisionB, revisionC), revisionsModifyingProperty.get("prop1").get(1));
        assertEquals(Set.of(revisionB), revisionsModifyingProperty.get("prop2").get(1));
        assertEquals(Set.of(revisionA), revisionsModifyingProperty.get("_deleted").get(1));
    }

    @Test
    public void testClassifyAndMapRevisionsMultipleDeleted() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'_deleted': {'" + revisionA + "': 'false', '" + revisionB + "': 'true', '" + revisionC + "': 'false'}," +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = NodeDocumentRevisionCleaner.getPropertiesModifiedByRevision();
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = NodeDocumentRevisionCleaner.getRevisionsModifyingPropertyByCluster();
        assertEquals(Set.of(revisionA, revisionB, revisionC), revisionsModifyingProperty.get("_deleted").get(1));
    }

    @Test
    public void testClassifyAndMapRevisionsAndPropertiesWithoutDeleted() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3'}, " +
                "'prop2': {'" + revisionB + "': 'value4'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
            "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = NodeDocumentRevisionCleaner.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = NodeDocumentRevisionCleaner.getRevisionsModifyingPropertyByCluster();
        assertEquals(Set.of(revisionA, revisionB, revisionC), revisionsModifyingProperty.get("prop1").get(1));
        assertEquals(Set.of(revisionB), revisionsModifyingProperty.get("prop2").get(1));
    }

    @Test
    public void testClassifyAndMapRevisionsAndPropertiesNotCommitted() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);
        Revision revisionD = new Revision(444444444L, 0, 1);

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3', '" + revisionD + "': 'value5'}, " +
                "'prop2': {'" + revisionB + "': 'value4', '" + revisionD + "': 'value5'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c', '" + revisionD + "': 'nc'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        // Modifications done in revisionD should be ignored, as it is not committed
        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = NodeDocumentRevisionCleaner.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));
        assertNull(propertiesModifiedByRevision.get(revisionD));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = NodeDocumentRevisionCleaner.getRevisionsModifyingPropertyByCluster();
        assertEquals(Set.of(revisionA, revisionB, revisionC), revisionsModifyingProperty.get("prop1").get(1));
        assertEquals(Set.of(revisionB), revisionsModifyingProperty.get("prop2").get(1));
    }

    @Test
    public void testClassifyAndMapRevisionsAndPropertiesDifferentClusters() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 2);
        Revision revisionC = new Revision(333333333L, 0, 3);
        Revision revisionD = new Revision(444444444L, 0, 1);

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3', '" + revisionD + "': 'value5'}, " +
                "'prop2': {'" + revisionB + "': 'value4', '" + revisionD + "': 'value5'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c', '" + revisionD + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        NodeDocumentRevisionCleaner.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = NodeDocumentRevisionCleaner.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionD));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = NodeDocumentRevisionCleaner.getRevisionsModifyingPropertyByCluster();
        assertEquals(Set.of(revisionA, revisionD), revisionsModifyingProperty.get("prop1").get(1));
        assertEquals(Set.of(revisionB), revisionsModifyingProperty.get("prop1").get(2));
        assertEquals(Set.of(revisionC), revisionsModifyingProperty.get("prop1").get(3));
        assertEquals(Set.of(revisionD), revisionsModifyingProperty.get("prop2").get(1));
        assertEquals(Set.of(revisionB), revisionsModifyingProperty.get("prop2").get(2));
        assertNull(revisionsModifyingProperty.get("prop2").get(3));
    }

    private void prepareDocumentMock(String jsonProperties) throws IOException {
        String json = jsonProperties.replace("'", "\"");

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, String>> data = objectMapper.readValue(json, new TypeReference<>() {});

        SortedMap<String, Object> entries = new TreeMap<>();
        SortedMap<Revision, String> allRevisions = new TreeMap<>(StableRevisionComparator.INSTANCE);
        for (Map.Entry<String, Map<String, String>> entry : data.entrySet()) {
            String property = entry.getKey();
            Map<String, String> revisions = entry.getValue();

            SortedMap<Revision, String> sortedRevisions = new TreeMap<>(StableRevisionComparator.INSTANCE);
            for (Map.Entry<String, String> revisionEntry : revisions.entrySet()) {
                String revisionStr = revisionEntry.getKey();
                String value = revisionEntry.getValue();

                String[] parts = revisionStr.split("-");
                // The timestamp part of the revision string (first part) is parsed as hexadecimal (radix 16)
                long timestamp = Long.parseLong(parts[0].substring(1), 16);
                int counter = Integer.parseInt(parts[1]);
                int clusterId = Integer.parseInt(parts[2]);

                Revision revision = new Revision(timestamp, counter, clusterId);
                sortedRevisions.put(revision, value);

                // Add all revisions to the "_revisions" list
                allRevisions.put(revision, value);
            }
            entries.put(property, sortedRevisions);
        }

        //System.out.println("ENTRIES::" + entries);
        //System.out.println("DATA::" + data);

        workingDocument.data = entries;
    }

    private void prepareCheckpointsMock(String jsonCheckpoints) throws IOException {
        String json = jsonCheckpoints.replace("'", "\"");

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Object>> data = objectMapper.readValue(json, new TypeReference<>() {});

        SortedMap<Revision, Checkpoints.Info> checkpoints = new TreeMap<>(StableRevisionComparator.REVERSE);
        for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
            String checkpointStr = entry.getKey();
            Map<String, Object> checkpointData = entry.getValue();

            String[] parts = checkpointStr.split("-");
            long timestamp = Long.parseLong(parts[0].substring(1), 16);
            int counter = Integer.parseInt(parts[1]);
            int clusterId = Integer.parseInt(parts[2]);

            Revision checkpoint = new Revision(timestamp, counter, clusterId);
            String checkpointDataJson = objectMapper.writeValueAsString(checkpointData);

            Checkpoints.Info info = Checkpoints.Info.fromString(checkpointDataJson);
            checkpoints.put(checkpoint, info);
        }

        Mockito.when(documentNodeStore.getCheckpoints()).thenReturn(this.checkpoints);
        Mockito.when(documentNodeStore.getCheckpoints().getCheckpoints()).thenReturn(checkpoints);

        /*Document cdoc = SETTINGS.newDocument(documentStore);
        Mockito.when(documentNodeStore.getCheckpoints()).thenReturn(this.checkpoints);
        Mockito.when(documentStore.find(Collection.SETTINGS, "checkpoint", 0)).thenReturn(cdoc);*/
    }
}