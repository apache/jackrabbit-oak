package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.guava.common.base.Strings;

public class LongIdMappingBlobStore extends IdMappingBlobStore {

    private static int next;

    @Override
    protected String generateId() {
        return Strings.repeat("0", Segment.BLOB_ID_SMALL_LIMIT) + Integer.toString(next++);
    }

}
