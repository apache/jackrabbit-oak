package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.InMemoryDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

public class CompositeDataStoreDelegateTest {
    private DataStoreProvider ds;
    private String defaultRole = "local1";
    private Map<String, Object> defaultConfig = Maps.newConcurrentMap();

    @Before
    public void setup() {
        ds = createDataStoreProvider(defaultRole);
        defaultConfig.put("prop1", "val1");
    }

    DataStoreProvider createDataStoreProvider(final String role) {
        return new DataStoreProvider() {
            DataStore ds = new InMemoryDataStore();
            @Override
            public DataStore getDataStore() {
                return ds;
            }

            @Override
            public String getRole() {
                return role;
            }
        };
    }

    void validate(CompositeDataStoreDelegate delegate,
                  DataStoreProvider ds,
                  String role,
                  Map<String, ?> config) {
        assertNotNull(delegate);
        assertEquals(ds, delegate.getDataStore());
        assertEquals(role, delegate.getRole());
        if (null != config) {
            assertEquals(config.size(), delegate.getConfig().size());
            for (String key : config.keySet()) {
                assertTrue(delegate.getConfig().containsKey(key));
                assertEquals(config.get(key), delegate.getConfig().get(key));
            }
        }
        else {
            assertEquals(1, delegate.getConfig().size()); // Should have the role only
        }
    }

    @Test
    public void testBuildCompositeDataStoreDelegate() {
        CompositeDataStoreDelegate delegate =
                CompositeDataStoreDelegate.builder(ds)
                        .withConfig(defaultConfig)
                        .build();

        validate(delegate, ds, defaultRole, defaultConfig);
    }

    @Test
    public void testBuildWithNullDataStoreProviderFails() {
        CompositeDataStoreDelegate delegate =
                CompositeDataStoreDelegate.builder(null)
                        .withConfig(defaultConfig)
                        .build();
        assertNull(delegate);
    }

    @Test
    public void testBuildWithoutConfig() {
        CompositeDataStoreDelegate delegate =
                CompositeDataStoreDelegate.builder(ds)
                        .build();
        Map<String, Object> cfg = Maps.newHashMap();
        cfg.put(DataStoreProvider.ROLE, defaultRole);
        validate(delegate, ds, defaultRole, cfg);
    }

    @Test
    public void testBuildWithNullConfig() {
        CompositeDataStoreDelegate delegate =
                CompositeDataStoreDelegate.builder(ds)
                        .withConfig(null)
                        .build();

        validate(delegate, ds, defaultRole, null);
    }

    @Test
    public void testBuildWithEmptyConfig() {
        Map<String, Object> cfg = Maps.newHashMap();
        CompositeDataStoreDelegate delegate =
                CompositeDataStoreDelegate.builder(ds)
                        .withConfig(cfg)
                        .build();

        validate(delegate, ds, defaultRole, cfg);
    }
}