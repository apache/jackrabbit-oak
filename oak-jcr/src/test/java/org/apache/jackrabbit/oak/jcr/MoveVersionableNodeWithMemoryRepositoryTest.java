package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.MEMORY_NS;

/**
 * Test for moving versionable nodes over deleted versionable nodes.
 * Will run all of the tests in MoveVersionableNodeWithSegmentTarRepositoryTestBase using the MEMORY_NS fixture.
 */
public class MoveVersionableNodeWithMemoryRepositoryTest extends MoveVersionableNodeWithSegmentTarRepositoryTestBase {

    public MoveVersionableNodeWithMemoryRepositoryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> memoryFixture() {
        return NodeStoreFixtures.asJunitParameters(singleton(MEMORY_NS));
    }
}
