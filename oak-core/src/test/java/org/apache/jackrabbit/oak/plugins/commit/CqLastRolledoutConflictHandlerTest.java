package org.apache.jackrabbit.oak.plugins.commit;

import static java.util.Calendar.DATE;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Calendar;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class CqLastRolledoutConflictHandlerTest {

    private CqLastRolledoutConflictHandler handler;
    private NodeBuilder nb;
    private String CQ_LAST_ROLLEDOUT = "cq:lastRolledout";

    @Before
    public void before() {
        handler = new CqLastRolledoutConflictHandler();
        nb = mock(NodeBuilder.class);
    }

    @Test
    public void testIgnoredProperty() {
        String propertyName = "ignored";
        PropertyState ours = createDateProperty(propertyName);
        PropertyState base = createDateProperty(propertyName);
        ThreeWayConflictHandler.Resolution resolution = handler.addExistingProperty(nb, ours, base);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        resolution = handler.changeDeletedProperty(nb, ours, base);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
        verifyNoInteractions(nb);
    }

    @Test
    public void testOtherTypeOfConflict() {
        PropertyState base = createDateProperty(CQ_LAST_ROLLEDOUT);
        PropertyState ours = createDateProperty(CQ_LAST_ROLLEDOUT);
        PropertyState theirs = createDateProperty(CQ_LAST_ROLLEDOUT);
        ThreeWayConflictHandler.Resolution resolution = handler.changeChangedProperty(nb, ours, theirs, base);
        assertSame(ThreeWayConflictHandler.Resolution.IGNORED, resolution);
    }

    @Test
    public void testChangeDeletedProperty() {
        PropertyState base = createDateProperty(CQ_LAST_ROLLEDOUT);
        PropertyState ours = createDateProperty(CQ_LAST_ROLLEDOUT);
        ThreeWayConflictHandler.Resolution resolution = handler.changeDeletedProperty(nb, ours, base);
        assertSame(ThreeWayConflictHandler.Resolution.OURS, resolution);
    }

    @NotNull
    public static PropertyState createDateProperty(@NotNull String name) {
        String now = ISO8601.format(Calendar.getInstance());
        return PropertyStates.createProperty(name, now, DATE);
    }
}