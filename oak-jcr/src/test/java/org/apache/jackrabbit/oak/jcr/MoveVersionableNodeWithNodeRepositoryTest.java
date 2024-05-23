package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;

/**
 * Test for moving versionable nodes over deleted versionable nodes.
 * Will run all the tests in MoveVersionableNodeWithSegmentTarRepositoryTestBase using the DOCUMENT_NS fixture.
 */
public class MoveVersionableNodeWithNodeRepositoryTest extends MoveVersionableNodeWithSegmentTarRepositoryTestBase {

    public MoveVersionableNodeWithNodeRepositoryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> memoryFixture() {
        return NodeStoreFixtures.asJunitParameters(singleton(DOCUMENT_NS));
    }
}
