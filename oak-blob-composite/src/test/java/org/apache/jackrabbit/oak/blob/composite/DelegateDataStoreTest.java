package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.InMemoryDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

public class DelegateDataStoreTest {
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

    void validate(DelegateDataStore delegate,
                  DataStoreProvider ds,
                  String role,
                  Map<String, ?> config) {
        assertNotNull(delegate);
        assertEquals(ds, delegate.getDataStore());
        assertEquals(role, delegate.getRole());
        if (null != config) {
            if (config.containsKey(DataStoreProvider.ROLE)) {
                assertEquals(config.size(), delegate.getConfig().size());
            }
            else {
                assertEquals(config.size()+1, delegate.getConfig().size());
            }
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
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(defaultConfig)
                        .build();

        validate(delegate, ds, defaultRole, defaultConfig);
    }

    @Test
    public void testBuildWithNullDataStoreProviderFails() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(null)
                        .withConfig(defaultConfig)
                        .build();
        assertNull(delegate);
    }

    @Test
    public void testBuildWithoutConfig() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .build();
        Map<String, Object> cfg = Maps.newHashMap();
        cfg.put(DataStoreProvider.ROLE, defaultRole);
        validate(delegate, ds, defaultRole, cfg);
    }

    @Test
    public void testBuildWithNullConfig() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(null)
                        .build();

        validate(delegate, ds, defaultRole, null);
    }

    @Test
    public void testBuildWithEmptyConfig() {
        Map<String, Object> cfg = Maps.newHashMap();
        DelegateDataStore delegate =
                DelegateDataStore.builder(ds)
                        .withConfig(cfg)
                        .build();

        validate(delegate, ds, defaultRole, cfg);
    }

//    @Test
//    public void testApplyCompositeDataStoreConfig() {
//        DelegateDataStore delegate = DelegateDataStore.builder(ds).withConfig(defaultConfig).build();
//        assertFalse(delegate.isReadOnly());
//
//        assertTrue(delegate.applyCompositeDataStoreConfig("readOnly:true,p1:v1"));
//
//        assertTrue(delegate.isReadOnly());
//        assertEquals("v1", delegate.getConfig().get("p1"));
//    }
//
//    @Test
//    public void testApplyCompositeDataStoreConfigNullString() {
//        DelegateDataStore delegate = DelegateDataStore.builder(ds).withConfig(defaultConfig).build();
//        assertFalse(delegate.applyCompositeDataStoreConfig(null));
//    }
//
//    @Test
//    public void testApplyCompositeDataStoreConfigEmptyString() {
//        DelegateDataStore delegate = DelegateDataStore.builder(ds).withConfig(defaultConfig).build();
//        assertFalse(delegate.applyCompositeDataStoreConfig(""));
//    }
//
//    @Test
//    public void testApplyCompositeDataStoreConfigInvalidFormat() {
//        DelegateDataStore delegate = DelegateDataStore.builder(ds).withConfig(defaultConfig).build();
//        assertFalse(delegate.isReadOnly());
//
//        assertFalse(delegate.applyCompositeDataStoreConfig("readOnly=true"));
//        assertFalse(delegate.isReadOnly());
//
//        assertFalse(delegate.applyCompositeDataStoreConfig("invalid"));
//        assertFalse(delegate.isReadOnly());
//    }

    @Test
    public void testIsReadOnly() {
        Map<String, Object> cfg = Maps.newHashMap();
        DelegateDataStore delegate;

        // Test forms of true
        List<String> formsOfTrue = Lists.newArrayList();
        formsOfTrue.add("true");
        formsOfTrue.add("True");
        formsOfTrue.add("TRUE");
        for (String ro : formsOfTrue) {
            cfg.put("readOnly", ro);
            delegate = DelegateDataStore.builder(ds)
                    .withConfig(cfg)
                    .build();
            assertTrue(delegate.isReadOnly());
        }

        // Test forms of false
        List<String> formsOfFalse = Lists.newArrayList();
        formsOfFalse.add("false");
        formsOfFalse.add("False");
        formsOfFalse.add("FALSE");
        formsOfFalse.add("something else");
        for (String ro : formsOfFalse) {
            cfg.put("readOnly", ro);
            delegate = DelegateDataStore.builder(ds)
                    .withConfig(cfg)
                    .build();
            assertFalse(delegate.isReadOnly());
        }
    }
}