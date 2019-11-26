package org.apache.jackrabbit.oak.plugins.blob.datastore;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.jcr.RepositoryException;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.CompositeDataStoreCache;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class FSBackendTest  {

	
	 protected static final Logger LOG = LoggerFactory.getLogger(FSBackendTest.class);

	    @Rule
	    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

	    private Properties props;
	    protected FSBackend backend;
	    protected String dataStoreDir;
	    protected DataStore ds;
	    /**
	     * Array of hexadecimal digits.
	     */
	    private static final char[] HEX = "0123456789abcdef".toCharArray();

	    /**
	     * The digest algorithm used to uniquely identify records.
	     */
	    protected String DIGEST = System.getProperty("ds.digest.algorithm", "SHA-256");
	    

	    @Before
	    public void setUp() throws Exception {
	    	dataStoreDir = folder.newFolder().getAbsolutePath();
	        props = new Properties();
	        props.setProperty("cacheSize", "0");
	        props.setProperty("fsBackendPath", dataStoreDir);
	        ds = createDataStore();
	        backend = (FSBackend)((CachingFileDataStore) ds).getBackend();
	    }

	    protected DataStore createDataStore() throws RepositoryException {
	        CachingFileDataStore ds = null;
	        try {
	            ds = new CachingFileDataStore();
	            Map<String, ?> config = DataStoreUtils.getConfig();
	            props.putAll(config);
	            PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
	            ds.setProperties(props);
	            ds.init(dataStoreDir);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return ds;
	    }
     
	    /**
	     * Testcase to validate mixed scenario use of {@link DataStore}.
	     */
	    @Test
        public void testSingleThreadFSBackend() {
	        try {
	            long start = System.currentTimeMillis();
	            LOG.info("Testcase: " + this.getClass().getName()
	                + "#testSingleThread, testDir=" + dataStoreDir);
	            doTestSingleThreadBackend();
	            LOG.info("Testcase: " + this.getClass().getName()
	                + "#testSingleThread finished, time taken = ["
	                + (System.currentTimeMillis() - start) + "]ms");
	        } catch (Exception e) {
	            LOG.error("error:", e);
	            fail(e.getMessage());
	        }
	    }
	    
	    /**
	     * Testcase to validate mixed scenario use of {@link DataStore} in
	     * multi-threaded concurrent environment.
	     */
        @Test
	    public void testMultiThreadedFSBackend() {
	        try {
	            long start = System.currentTimeMillis();
	            LOG.info("Testcase: " + this.getClass().getName()
	                + "#testMultiThreaded, testDir=" + dataStoreDir);
	            doTestMultiThreadedBackend();
	            LOG.info("Testcase: " + this.getClass().getName()
	                + "#testMultiThreaded finished, time taken = ["
	                + (System.currentTimeMillis() - start) + "]ms");
	        } catch (Exception e) {
	            LOG.error("error:", e);
	            fail(e.getMessage());
	        }
	    }

	    @After
	    public void tearDown() {
	    	try {
				ds.close();
			} catch (DataStoreException e) {
				LOG.error("error:", e);
	            fail(e.getMessage());
			}
	    }
	    
	    /**
	     * Method to validate mixed scenario use of {@link DataStore}.
	     */
	    protected void doTestSingleThreadBackend() throws Exception {
	        doTestMultiThreaded(ds, 1);
	    }

	    /**
	     * Method to validate mixed scenario use of {@link DataStore} in
	     * multi-threaded concurrent environment.
	     */	    
	    protected void doTestMultiThreadedBackend() throws Exception {
	        doTestMultiThreaded(ds, 4);
	    }

	    /**
	     * Method to run {@link FSBackendTest#doTest(DataStore, int)} in multiple
	     * concurrent threads.
	     */	    
	    protected void doTestMultiThreaded(final DataStore ds, int threadCount)
	            throws Exception {
	        final Exception[] exception = new Exception[1];
	        Thread[] threads = new Thread[threadCount];
	        for (int i = 0; i < threadCount; i++) {
	            final int x = i;
	            Thread t = new Thread() {
	                public void run() {
	                    try {
	                        doTest(ds, x);
	                    } catch (Exception e) {
	                        exception[0] = e;
	                    }
	                }
	            };
	            threads[i] = t;
	            t.start();
	        }
	        for (int i = 0; i < threadCount; i++) {
	            threads[i].join();
	        }
	        if (exception[0] != null) {
	            throw exception[0];
	        }
	    }
	    
	    /**
	     * Method to assert record while writing and deleting record from FSBackend
	     */
	    void doTest(DataStore ds, int offset) throws Exception {
	        String path = dataStoreDir + "/repository/datastore";
	        ArrayList<DataRecord> list = new ArrayList<DataRecord>();
	        File rootDirectory = new File(path);
	        File tmp = new File(rootDirectory, "tmp");
	        HashMap<DataRecord, Integer> map = new HashMap<DataRecord, Integer>();
	        for (int i = 0; i < 10; i++) {
	            int size = 100000 - (i * 100);
	            RandomInputStream in = new RandomInputStream(size + offset, size);
	            TransientFileFactory fileFactory = TransientFileFactory.getInstance();
	            File tmpFile = fileFactory.createTransientFile("upload", null, tmp);
	            MessageDigest digest = MessageDigest.getInstance(DIGEST);
	            OutputStream output = new DigestOutputStream(new FileOutputStream(tmpFile), digest);
	            try {
	                 IOUtils.copyLarge(in, output);
	            } finally {
	                output.close();
	            }
	            DataIdentifier identifier = new DataIdentifier(encodeHexString(digest.digest()));
	            backend.write(identifier, tmpFile);
	            DataRecord rec = ds.getRecordIfStored(identifier);
	            Assert.assertEquals(rec.getIdentifier(), identifier);
	            list.add(rec);
	            map.put(rec, size);
	            System.out.println("Write "+i+" record " +rec);
	        }
	        Random random = new Random(1);
	        for (int i = 0; i < list.size(); i++) {
	            int pos = random.nextInt(list.size());
	            DataRecord rec1 = list.get(pos);
	            DataIdentifier di = rec1.getIdentifier();
	            backend.deleteRecord(di);
	            DataRecord rec2 = ds.getRecordIfStored(di);
	            Assert.assertNotEquals(rec1, rec2);
	        }
	    }

	    protected static String encodeHexString(byte[] value) {
	        char[] buffer = new char[value.length * 2];
	        for (int i = 0; i < value.length; i++) {
	            buffer[2 * i] = HEX[(value[i] >> 4) & 0x0f];
	            buffer[2 * i + 1] = HEX[value[i] & 0x0f];
	        }
	        return new String(buffer);
	    }
}
