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

package org.apache.jackrabbit.oak.plugins.blob;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.After;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.jackrabbit.oak.plugins.blob.SharedDataStore.Type.SHARED;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SharedDataStoreMarkSweepGarbageCollectorTest {

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

  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

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
      new DefaultStatisticsProvider(executor)
    );
  }

  @After
  public void tear() {
    new ExecutorCloser(executor).close();
  }

  @Test
  public void markAndSweepShouldFailIfNotAllRepositoriesHaveMarkedReferencesAvailable() throws Exception {
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
