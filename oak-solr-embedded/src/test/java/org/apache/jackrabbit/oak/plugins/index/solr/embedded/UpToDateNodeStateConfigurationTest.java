
package org.apache.jackrabbit.oak.plugins.index.solr.embedded;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Testcase for {@link UpToDateNodeStateConfiguration}
 */
public class UpToDateNodeStateConfigurationTest {

    private NodeStore store;

    @Before
    public void setUp() throws Exception {
        MicroKernel microKernel = new MicroKernelImpl();
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{} +\"y\":{} +\"z\":{} " +
                "+\"oak:index\":{\"solrIdx\":{\"coreName\":\"cn\", \"solrHome\":\"sh\", \"solrConfig\":\"sc\"}} ";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        store = new KernelNodeStore(microKernel);
    }

    @Test
    public void testExistingPath() throws Exception {
        String path = "oak:index/solrIdx";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        assertEquals("cn", upToDateNodeStateConfiguration.getCoreName()); // property defined in the node state
        assertEquals("path_exact", upToDateNodeStateConfiguration.getPathField()); // using default as this property not defined in the node state
    }

    @Test
    public void testNonExistingPath() throws Exception {
        String path = "some/path/to/oak:index/solrIdx";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        assertNull(upToDateNodeStateConfiguration.getCoreName());
    }

    @Test
    public void testNodeStateNotFound() throws Exception {
        String path = "some/path/to/somewhere/unknown";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        assertFalse(upToDateNodeStateConfiguration.getConfigurationNodeState().exists());
    }
}
