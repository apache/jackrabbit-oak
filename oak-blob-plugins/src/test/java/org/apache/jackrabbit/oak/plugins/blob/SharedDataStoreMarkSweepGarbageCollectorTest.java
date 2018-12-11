package org.apache.jackrabbit.oak.plugins.blob;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.jackrabbit.oak.plugins.blob.SharedDataStore.Type.SHARED;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SharedDataStoreMarkSweepGarbageCollectorTest {

  @Mock
  private Executor executor;

  @Mock
  private MockGarbageCollectableSharedDataStore blobStore;

  @Mock
  private BlobReferenceRetriever marker;

  @Mock
  private Whiteboard whiteboard;

  @Mock
  private Tracker<CheckpointMBean> tracker;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private MarkSweepGarbageCollector collector;

  @Mock
  private CheckpointMBean checkpointMBean;

  @Before
  public void setUp() throws IOException {
    when(whiteboard.track(CheckpointMBean.class)).thenReturn(tracker);
    when(tracker.getServices()).thenReturn(ImmutableList.of(checkpointMBean));

    when(blobStore.getType()).thenReturn(SHARED);

    collector = new MarkSweepGarbageCollector(
      marker,
      blobStore,
      executor,
      MarkSweepGarbageCollector.TEMP_DIR,
      1,
      0L,
      "repo",
      whiteboard,
      null
    );
  }

  @Test
  public void markAndSweepShouldFailIfNotAllRepositoriesHaveMarkedReferencesAvailable() throws Exception {
    exception.expect(IOException.class);
    exception.expectMessage(CoreMatchers.containsString("Not all repositories have marked references available"));

    setupSharedDataRecords("REPO1", "REPO2");

    collector.markAndSweep(false, true);

    assertThat(collector.getOperationStats().numDeleted(), is(0L));
    assertThat(collector.getOperationStats().getFailureCount(), is(1L));
  }

  @Test
  public void markAndSweepShouldSucceedWhenAllRepositoriesAreAvailable() throws Exception {
    setupSharedDataRecords("REPO1", "REPO1");
    when(blobStore.getAllChunkIds(0L)).thenReturn(ImmutableList.<String>of().iterator());

    collector.markAndSweep(false, true);

    assertThat(collector.getOperationStats().numDeleted(), is(0L));
    assertThat(collector.getOperationStats().getFailureCount(), is(0L));
  }

  private void setupSharedDataRecords(final String refRepoId, final String repoRepoId) throws DataStoreException {
    DataRecord refDataRecord = mock(DataRecord.class);
    when(refDataRecord.getIdentifier()).thenReturn(new DataIdentifier("references-" + refRepoId));
    when(refDataRecord.getStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(refDataRecord.getLastModified()).thenReturn(10L);

    DataRecord repoDataRecord = mock(DataRecord.class);
    when(repoDataRecord.getIdentifier()).thenReturn(new DataIdentifier("repository-" + repoRepoId));

    List<DataRecord> refs = ImmutableList.of(refDataRecord);
    List<DataRecord> repos = ImmutableList.of(repoDataRecord);

    when(blobStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REFERENCES.getType())).thenReturn(refs);
    when(blobStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getType())).thenReturn(repos);
    when(blobStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.MARKED_START_MARKER.getType())).thenReturn(refs);
  }

  private interface MockGarbageCollectableSharedDataStore extends GarbageCollectableBlobStore, SharedDataStore {
  }
}