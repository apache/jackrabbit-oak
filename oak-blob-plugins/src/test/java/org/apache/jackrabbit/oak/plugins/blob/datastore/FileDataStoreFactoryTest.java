package org.apache.jackrabbit.oak.plugins.blob.datastore;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.service.component.ComponentContext;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileDataStoreFactoryTest {
    @Rule
    public OsgiContext context = new OsgiContext();

    @Before
    public void setup() {
        //Map<String, Object> properties = Maps.newConcurrentMap();
        //properties.put (DataStoreProvider.ROLE, "local");
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        //context.registerInjectActivateService(AbstractDataStoreFactory.class, properties);
    }

    private void activateService() {
        activateService(new FileDataStoreFactory(), null);
    }

    private void activateService(final AbstractDataStoreFactory factory) {
        activateService(factory, null);
    }

    private void activateService(final String role) {
        activateService(new FileDataStoreFactory(), role);
    }

    private void activateService(final AbstractDataStoreFactory factory, final String role) {
        Map<String, Object> config = Maps.newHashMap();
        if (null != role) {
            config.put(DataStoreProvider.ROLE, role);
        }
        context.registerInjectActivateService(factory, config);
    }

    @Test
    public void testActivate() throws IOException {
        activateService("local");
        DataStoreProvider dsp = context.getService(DataStoreProvider.class);
        assertNotNull(dsp);
        DataStore ds = dsp.getDataStore();
        assertNotNull(ds);
        if (! (ds instanceof OakFileDataStore) && ! (ds instanceof CachingFileDataStore)) {
            fail();
        }
    }

    @Test
    public void testActivateWithoutRole() throws IOException {
        activateService();
        assertNull(context.getService(DataStoreProvider.class));
    }

    @Test
    public void testActivateWithoutDataStore() throws IOException {
        FileDataStoreFactory factory = mock(FileDataStoreFactory.class);
        when(factory.createDataStore(any(ComponentContext.class), anyMapOf(String.class, Object.class))).thenReturn(null);

        activateService(factory, "local");
        assertNull(context.getService(DataStoreProvider.class));
    }

    @Test
    public void testActivateMultiple() throws IOException {
        Set<String> roles = Sets.newHashSet("local1", "local2", "cloud1");
        for (String role : roles) {
            activateService(role);
        }
        DataStoreProvider[] dsps = context.getServices(DataStoreProvider.class, null);
        assertEquals(3, dsps.length);
        for (DataStoreProvider dsp : dsps) {
            assertTrue(roles.contains(dsp.getRole()));
        }

        for (String role : roles) {
            dsps = context.getServices(DataStoreProvider.class, String.format("(role=%s)", role));
            assertEquals(1, dsps.length);
            DataStoreProvider dsp = dsps[0];
            assertEquals(role, dsp.getRole());
        }

        // Invalid role
        dsps = context.getServices(DataStoreProvider.class, "(role=notarole)");
        assertEquals(0, dsps.length);
    }
}
