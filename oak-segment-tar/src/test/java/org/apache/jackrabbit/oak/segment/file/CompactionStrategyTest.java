package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class CompactionStrategyTest {

    private static final Throwable MARKER_THROWABLE =
            new RuntimeException("We pretend that something went horribly wrong.");

    @Test
    public void compactionIsAbortedOnAnyThrowable() throws IOException {
        MemoryStore store = new MemoryStore();
        CompactionStrategy.Context throwingContext = Mockito.mock(CompactionStrategy.Context.class);
        when(throwingContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        when(throwingContext.getRevisions()).thenReturn(store.getRevisions());
        when(throwingContext.getGCOptions()).thenThrow(MARKER_THROWABLE);

        try {
            final CompactionResult compactionResult = new FullCompactionStrategy().compact(throwingContext);
            assertThat("Compaction should be properly aborted.", compactionResult.isSuccess(), is(false));
        } catch (Throwable e) {
            if (e == MARKER_THROWABLE) {
                fail("The marker throwable was not caught by the CompactionStrategy and therefore not properly aborted.");
            } else {
                throw new IllegalStateException("The test likely needs to be adjusted.", e);
            }
        }
    }
}