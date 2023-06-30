package org.apache.jackrabbit.oak.plugins.document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DocumentRevisionCleanupHelperTest {

    @Mock
    DocumentStore documentStore;

    @Mock
    DocumentNodeStore documentNodeStore;

    @Mock
    Checkpoints checkpoints;

    @Mock
    NodeDocument workingDocument;

    DocumentRevisionCleanupHelper documentRevisionCleanupHelper;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        workingDocument = Mockito.mock(NodeDocument.class);

        Mockito.when(documentStore.find(Mockito.eq(NODES), Mockito.anyString())).thenReturn(workingDocument);
        documentRevisionCleanupHelper = new DocumentRevisionCleanupHelper(documentStore, documentNodeStore, "/");
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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();
        documentRevisionCleanupHelper.markCheckpointRevisionsToPreserve();

        // The revisions blocked should be:
        //  - r105000000-0-1 (blocked by checkpoint r109000000-0-1)
        //  - r115000000-0-1 (blocked by checkpoint r119000000-0-1)

        assertEquals(Set.of(revisionB, revisionD), documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1));


        /*assertEquals(Set.of(revisionC), documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revisionA, revisionB, revisionC), documentRevisionCleanupHelper.getCandidateRevisionsToClean().get(1));*/
    }

    @Test
    public void testInitializeCleanupProcessMultipleClusters() throws IOException {
        Revision revisionA = Revision.fromString("r100000000-0-1");
        Revision revisionB = Revision.fromString("r105000000-0-2");
        Revision revisionC = Revision.fromString("r110000000-0-3");
        Revision revisionD = Revision.fromString("r115000000-0-1");
        Revision revisionE = Revision.fromString("r120000000-0-2");
        Revision revisionF = Revision.fromString("r125000000-0-3");

        Revision checkpoint1 = Revision.fromString("r109000000-0-1");
        Revision checkpoint2 = Revision.fromString("r119000000-0-1");

        String jsonProperties = "{" +
                "'prop1': {'" + revisionA + "': 'value1', '" + revisionB + "': 'value2', '" + revisionC + "': 'value3', '" + revisionD + "': 'value4', '" + revisionE + "': 'value5', '" + revisionF + "': 'value6'}, " +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c', '" + revisionC + "': 'c', '" + revisionD + "': 'c', '" + revisionE + "': 'c', '" + revisionF + "': 'c'}" +
                "}";
        String jsonCheckpoints = "{" +
                "'" + checkpoint1 + "': {'expires':'200000000','rv':'r109000000-0-1,r109000000-0-1'}," +
                "'" + checkpoint2 + "': {'expires':'200000000','rv':'r119000000-0-1,r109000000-0-1'}" +
                "}";


        prepareDocumentMock(jsonProperties);
        prepareCheckpointsMock(jsonCheckpoints);

        documentRevisionCleanupHelper.initializeCleanupProcess();

        // The revisions blocked should be:
        //

        assertEquals(Set.of(revisionC), documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revisionA, revisionB, revisionC), documentRevisionCleanupHelper.getCandidateRevisionsToClean().get(1));
    }

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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();
        documentRevisionCleanupHelper.markLastRevisionForEachProperty();

        assertFalse(documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertFalse(documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1).contains(revisionB));
        assertEquals(Set.of(revisionC), documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1));
        assertEquals(Set.of(revisionA, revisionB, revisionC), documentRevisionCleanupHelper.getCandidateRevisionsToClean().get(1));
    }

    @Test
    public void testFirstDeletedRevisionIsBlocked() throws IOException {
        Revision revisionA = new Revision(111111111L, 0, 1);
        Revision revisionB = new Revision(222222222L, 0, 1);
        Revision revisionC = new Revision(333333333L, 0, 1);

        String jsonProperties = "{" +
                "'_deleted': {'" + revisionA + "': 'false', '" + revisionB + "': 'true', '" + revisionC + "': 'false'}," +
                "'_revisions': {'" + revisionA + "': 'c', '" + revisionB + "': 'c', '" + revisionC + "': 'c'}" +
                "}";
        prepareDocumentMock(jsonProperties);

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        assertTrue(documentRevisionCleanupHelper.getBlockedRevisionsToKeep().get(1).contains(revisionA));
        assertEquals(Set.of(revisionA, revisionB, revisionC), documentRevisionCleanupHelper.getCandidateRevisionsToClean().get(1));
    }

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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = documentRevisionCleanupHelper.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1", "_deleted"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = documentRevisionCleanupHelper.getRevisionsModifyingProperty();
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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = documentRevisionCleanupHelper.getPropertiesModifiedByRevision();
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("_deleted"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = documentRevisionCleanupHelper.getRevisionsModifyingProperty();
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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = documentRevisionCleanupHelper.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = documentRevisionCleanupHelper.getRevisionsModifyingProperty();
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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        // Modifications done in revisionD should be ignored, as it is not committed
        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = documentRevisionCleanupHelper.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));
        assertNull(propertiesModifiedByRevision.get(revisionD));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = documentRevisionCleanupHelper.getRevisionsModifyingProperty();
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

        documentRevisionCleanupHelper.classifyAndMapRevisionsAndProperties();

        SortedMap<Revision, TreeSet<String>> propertiesModifiedByRevision = documentRevisionCleanupHelper.getPropertiesModifiedByRevision();
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionA));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionB));
        assertEquals(Set.of("prop1"), propertiesModifiedByRevision.get(revisionC));
        assertEquals(Set.of("prop1", "prop2"), propertiesModifiedByRevision.get(revisionD));

        SortedMap<String, SortedMap<Integer, TreeSet<Revision>>> revisionsModifyingProperty = documentRevisionCleanupHelper.getRevisionsModifyingProperty();
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

        Mockito.when(workingDocument.entrySet()).thenReturn(entries.entrySet());
        Mockito.when(workingDocument.get("_deleted")).thenReturn(entries.get("_deleted"));
        Mockito.when(workingDocument.get("_revisions")).thenReturn(allRevisions);
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