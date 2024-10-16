package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RemoteUtilitiesTest {
    @Test
    public void testValidEntryIndex() {
        UUID uuid = UUID.randomUUID();
        String name = RemoteUtilities.getSegmentFileName(
                RemoteUtilities.MAX_ENTRY_COUNT - 1,
                uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits()
        );
        assertEquals(uuid, RemoteUtilities.getSegmentUUID(name));
    }

  @Test
  public void testInvalidEntryIndex() {
    UUID uuid = UUID.randomUUID();
    String name = RemoteUtilities.getSegmentFileName(
            RemoteUtilities.MAX_ENTRY_COUNT,
            uuid.getMostSignificantBits(),
            uuid.getLeastSignificantBits()
    );
    assertNotEquals(uuid, RemoteUtilities.getSegmentUUID(name));
  }
}
