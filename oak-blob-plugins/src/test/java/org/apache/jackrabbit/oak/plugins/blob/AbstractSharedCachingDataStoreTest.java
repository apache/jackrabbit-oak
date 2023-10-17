package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class AbstractSharedCachingDataStoreTest {

    public static Logger LOG = LoggerFactory.getLogger(AbstractSharedCachingDataStore.class);
    public static long BACKEND_ACCESS_DELAY_MILLIS = 4;

    private AbstractSharedCachingDataStore ds;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    public void createDataStore() throws Exception {
        ds = new AbstractSharedCachingDataStore() {
            @Override
            public DataRecord addRecord(InputStream is) {
                try {
                    Thread.sleep(BACKEND_ACCESS_DELAY_MILLIS);
                } catch (InterruptedException e) {
                }
                return null;
            }

            @Override
            protected AbstractSharedBackend createBackend() {
                return new AbstractSharedBackend() {
                    @Override
                    public InputStream read(DataIdentifier identifier) throws DataStoreException {
                        throw new DataStoreException("Should not happen");
                    }

                    @Override
                    public void write(DataIdentifier identifier, File file) throws DataStoreException {
                        throw new DataStoreException("Should not happen");
                    }

                    @Override
                    public DataRecord getRecord(DataIdentifier id) throws DataStoreException {
                        try {
                            Thread.sleep(BACKEND_ACCESS_DELAY_MILLIS);
                        } catch (InterruptedException e) {
                        }
                        return new DataRecord() {
                            private final byte[] bytes = "12345".getBytes();

                            @Override
                            public DataIdentifier getIdentifier() {
                                return id;
                            }

                            @Override
                            public String getReference() {
                                return id.toString();
                            }

                            @Override
                            public long getLength() throws DataStoreException {
                                return bytes.length;
                            }

                            @Override
                            public InputStream getStream() throws DataStoreException {
                                return new ByteArrayInputStream(bytes);
                            }

                            @Override
                            public long getLastModified() {
                                return 1;
                            }
                        };
                    }

                    @Override
                    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
                        return null;
                    }

                    @Override
                    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
                        return null;
                    }

                    @Override
                    public boolean exists(DataIdentifier identifier) throws DataStoreException {
                        return false;
                    }

                    @Override
                    public void close() throws DataStoreException {
                    }

                    @Override
                    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
                    }

                    @Override
                    public void addMetadataRecord(InputStream input, String name) throws DataStoreException {

                    }

                    @Override
                    public void addMetadataRecord(File input, String name) throws DataStoreException {

                    }

                    @Override
                    public DataRecord getMetadataRecord(String name) {
                        return null;
                    }

                    @Override
                    public List<DataRecord> getAllMetadataRecords(String prefix) {
                        return null;
                    }

                    @Override
                    public boolean deleteMetadataRecord(String name) {
                        return false;
                    }

                    @Override
                    public void deleteAllMetadataRecords(String prefix) {
                    }

                    @Override
                    public boolean metadataRecordExists(String name) {
                        return false;
                    }

                    @Override
                    public void init() throws DataStoreException {
                    }
                };
            }

            @Override
            public int getMinRecordLength() {
                return 0;
            }
        };
        ds.init(folder.newFolder().getAbsolutePath());
    }

    @Test
    public void testGetRecordIfStored() throws Exception {
        final int iterations = 100;
        Field recordCacheSize = AbstractSharedCachingDataStore.class.getDeclaredField("RECORD_CACHE_SIZE");
        recordCacheSize.setAccessible(true);

        recordCacheSize.setLong(AbstractSharedCachingDataStore.class, 0);
        createDataStore();

        long start = System.nanoTime();
        for (int i = 0; i < iterations; ++i) {
            LOG.trace("" + ds.getRecordIfStored(new DataIdentifier("12345"))); // LOG.trace to avoid the call being optimised away
        }
        long timeUncached = System.nanoTime() - start;

        recordCacheSize.setLong(AbstractSharedCachingDataStore.class, 10000);
        createDataStore();

        start = System.nanoTime();
        DataIdentifier di = new DataIdentifier("12345");
        for (int i = 0; i < iterations; ++i) {
            LOG.trace("" + ds.getRecordIfStored(di)); // LOG.trace to avoid the call being optimised away
        }
        long timeCached = System.nanoTime() - start;
        assertTrue(String.format("timeCached: %d, timeUncached: %d", timeCached, timeUncached), 5 * timeCached < timeUncached);
    }
}