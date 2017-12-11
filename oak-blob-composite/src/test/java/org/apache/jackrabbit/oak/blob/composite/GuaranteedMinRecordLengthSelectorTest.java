package org.apache.jackrabbit.oak.blob.composite.delegate;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.InMemoryDataStore;
import org.apache.jackrabbit.oak.blob.composite.DelegateDataStore;
import org.apache.jackrabbit.oak.blob.composite.DelegateHandler;
import org.apache.jackrabbit.oak.blob.composite.DelegateMinRecordLengthSelector;
import org.apache.jackrabbit.oak.blob.composite.GuaranteedMinRecordLengthSelector;
import org.apache.jackrabbit.oak.blob.composite.IntelligentDelegateHandler;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GuaranteedMinRecordLengthSelectorTest {
    private DelegateHandler delegateHandler;
    private DelegateMinRecordLengthSelector minRecordLengthSelector;

    private static final String MRL_KEY = "minRecordLength";

    @Before
    public void setup() {
        delegateHandler = new IntelligentDelegateHandler();
        minRecordLengthSelector = new GuaranteedMinRecordLengthSelector();
    }

    private DelegateDataStore createDelegateDataStore(final String role, final Map<String, Object> config) {
        return new DelegateDataStore(new DataStoreProvider() {
            private InMemoryDataStore ds;
            @Override
            public DataStore getDataStore() {
                return ds;
            }

            @Override
            public String getRole() {
                return role;
            }
        }, config);
    }

    @Test
    public void testSingleDelegateReturnsDelegateMinRecLen() {
        String role = "role1";
        long len = 4096;
        Map<String, Object> config = Maps.newHashMap();
        config.put(DataStoreProvider.ROLE, role);
        config.put(MRL_KEY, len);
        delegateHandler.addDelegateDataStore(createDelegateDataStore("role1", config));
        assertEquals(len, minRecordLengthSelector.getMinRecordLength(delegateHandler));
    }

    @Test
    public void testMultipleWritableDelegatesReturnsOverallMinRecLen() {

    }

    @Test
    public void testGetMinRecLenIgnoresReadonlyDelegates() {

    }

    @Test
    public void testNullDataStoreReturnsNPE() {

    }
}
