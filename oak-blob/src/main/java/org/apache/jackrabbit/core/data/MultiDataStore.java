/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import javax.jcr.RepositoryException;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.core.fs.FileSystemResource;
import org.apache.jackrabbit.core.fs.local.LocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MultiDataStore can handle two independent DataStores.
 * <p>
 * <b>Attention:</b> You will lost the global single instance mechanism !
 * </p>
 * It can be used if you have two storage systems. One for fast access and a
 * other one like a archive DataStore on a slower storage system. All Files will
 * be added to the primary DataStore. On read operations first the primary
 * dataStore will be used and if no Record is found the archive DataStore will
 * be used. The GarabageCollector will only remove files from the archive
 * DataStore.
 * <p>
 * The internal MoveDataTask will be started automatically and could be
 * configured with the following properties.
 * <p>
 * The Configuration:
 * 
 * <pre>
 * &lt;DataStore class="org.apache.jackrabbit.core.data.MultiDataStore"&gt;
 *     &lt;param name="{@link #setMaxAge(int) maxAge}" value="60"/&gt;
 *     &lt;param name="{@link #setMoveDataTaskSleep(int) moveDataTaskSleep}" value="604800"/&gt;
 *     &lt;param name="{@link #setMoveDataTaskFirstRunHourOfDay(int) moveDataTaskFirstRunHourOfDay}" value="1"/&gt;
 *     &lt;param name="{@link #setSleepBetweenRecords(long) sleepBetweenRecords}" value="100"/&gt;
 *     &lt;param name="{@link #setDelayedDelete(boolean) delayedDelete}" value="false"/&gt;
 *     &lt;param name="{@link #setDelayedDeleteSleep(long) delayedDeleteSleep}" value="86400"/&gt;
 *     &lt;param name="primary" value="org.apache.jackrabbit.core.data.db.DbDataStore"&gt;
 *        &lt;param .../&gt;
 *     &lt;/param&gt;
 *     &lt;param name="archive" value="org.apache.jackrabbit.core.data.FileDataStore"&gt;
 *        &lt;param .../&gt;
 *     &lt;/param&gt;
 * &lt;/DataStore&gt;
 * </pre>
 * 
 * <ul>
 * <li><code>maxAge</code>: defines how many days the content will reside in the
 * primary data store. DataRecords that have been added before this time span
 * will be moved to the archive data store. (default = <code>60</code>)</li>
 * <li><code>moveDataTaskSleep</code>: specifies the sleep time of the
 * moveDataTaskThread in seconds. (default = 60 * 60 * 24 * 7, which equals 7
 * days)</li>
 * <li><code>moveDataTaskNextRunHourOfDay</code>: specifies the hour at which
 * the moveDataTaskThread initiates its first run (default = <code>1</code>
 * which means 01:00 at night)</li>
 * <li><code>sleepBetweenRecords</code>: specifies the delay in milliseconds
 * between scanning data records (default = <code>100</code>)</li>
 * <li><code>delayedDelete</code>: its possible to delay the delete operation on
 * the primary data store. The DataIdentifiers will be written to a temporary
 * file. The file will be processed after a defined sleep (see
 * <code>delayedDeleteSleep</code>) It's useful if you like to create a snapshot
 * of the primary data store backend in the meantime before the data will be
 * deleted. (default = <code>false</code>)</li>
 * <li><code>delayedDeleteSleep</code>: specifies the sleep time of the
 * delayedDeleteTaskThread in seconds. (default = 60 * 60 * 24, which equals 1
 * day). This means the delayed delete from the primary data store will be
 * processed after one day.</li>
 * </ul>
 */
public class MultiDataStore implements DataStore {

    /**
     * Logger instance
     */
    private static Logger log = LoggerFactory.getLogger(MultiDataStore.class);

    private DataStore primaryDataStore;
    private DataStore archiveDataStore;

    /**
     * Max Age in days.
     */
    private int maxAge = 60;

    /**
     * ReentrantLock that is used while the MoveDataTask is running.
     */
    private ReentrantLock moveDataTaskLock = new ReentrantLock();
    private boolean moveDataTaskRunning = false;
    private Thread moveDataTaskThread;

    /**
     * The sleep time in seconds of the MoveDataTask, 7 day default.
     */
    private int moveDataTaskSleep = 60 * 60 * 24 * 7;

    /**
     * Indicates when the next run of the move task is scheduled. The first run
     * is scheduled by default at 01:00 hours.
     */
    private Calendar moveDataTaskNextRun = Calendar.getInstance();

    /**
     * Its possible to delay the delete operation on the primary data store
     * while move task is running. The delete will be executed after defined
     * delayDeleteSleep.
     */
    private boolean delayedDelete = false;

    /**
     * The sleep time in seconds to delay remove operation on the primary data
     * store, 1 day default.
     */
    private long delayedDeleteSleep = 60 * 60 * 24;

    /**
     * File that holds the data identifiers if delayDelete is enabled.
     */
    private FileSystemResource identifiersToDeleteFile = null;

    private Thread deleteDelayedIdentifiersTaskThread;

    /**
     * Name of the file which holds the identifiers if deleayed delete is
     * enabled
     */
    private final String IDENTIFIERS_TO_DELETE_FILE_KEY = "identifiersToDelete";

    /**
     * The delay time in milliseconds between scanning data records, 100
     * default.
     */
    private long sleepBetweenRecords = 100;

    {
        if (moveDataTaskNextRun.get(Calendar.HOUR_OF_DAY) >= 1) {
            moveDataTaskNextRun.add(Calendar.DAY_OF_MONTH, 1);
        }
        moveDataTaskNextRun.set(Calendar.HOUR_OF_DAY, 1);
        moveDataTaskNextRun.set(Calendar.MINUTE, 0);
        moveDataTaskNextRun.set(Calendar.SECOND, 0);
        moveDataTaskNextRun.set(Calendar.MILLISECOND, 0);
    }

    /**
     * Setter for the primary dataStore
     * 
     * @param dataStore
     */
    public void setPrimaryDataStore(DataStore dataStore) {
        this.primaryDataStore = dataStore;
    }

    /**
     * Setter for the archive dataStore
     * 
     * @param dataStore
     */
    public void setArchiveDataStore(DataStore dataStore) {
        this.archiveDataStore = dataStore;
    }

    /**
     * Check if a record for the given identifier exists in the primary data
     * store. If not found there it will be returned from the archive data
     * store. If no record exists, this method returns null.
     * 
     * @param identifier
     *            data identifier
     * @return the record if found, and null if not
     */
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        if (moveDataTaskRunning) {
            moveDataTaskLock.lock();
        }
        try {
            DataRecord dataRecord = primaryDataStore.getRecordIfStored(identifier);
            if (dataRecord == null) {
                dataRecord = archiveDataStore.getRecordIfStored(identifier);
            }
            return dataRecord;
        } finally {
            if (moveDataTaskRunning) {
                moveDataTaskLock.unlock();
            }
        }
    }

    /**
     * Returns the identified data record from the primary data store. If not
     * found there it will be returned from the archive data store. The given
     * identifier should be the identifier of a previously saved data record.
     * Since records are never removed, there should never be cases where the
     * identified record is not found. Abnormal cases like that are treated as
     * errors and handled by throwing an exception.
     * 
     * @param identifier
     *            data identifier
     * @return identified data record
     * @throws DataStoreException
     *             if the data store could not be accessed, or if the given
     *             identifier is invalid
     */
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (moveDataTaskRunning) {
            moveDataTaskLock.lock();
        }
        try {
            return primaryDataStore.getRecord(identifier);
        } catch (DataStoreException e) {
            return archiveDataStore.getRecord(identifier);
        } finally {
            if (moveDataTaskRunning) {
                moveDataTaskLock.unlock();
            }
        }
    }

    /**
     * Creates a new data record in the primary data store. The given binary
     * stream is consumed and a binary record containing the consumed stream is
     * created and returned. If the same stream already exists in another
     * record, then that record is returned instead of creating a new one.
     * <p>
     * The given stream is consumed and <strong>not closed</strong> by this
     * method. It is the responsibility of the caller to close the stream. A
     * typical call pattern would be:
     * 
     * <pre>
     *     InputStream stream = ...;
     *     try {
     *         record = store.addRecord(stream);
     *     } finally {
     *         stream.close();
     *     }
     * </pre>
     * 
     * @param stream
     *            binary stream
     * @return data record that contains the given stream
     * @throws DataStoreException
     *             if the data store could not be accessed
     */
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        return primaryDataStore.addRecord(stream);
    }

    /**
     * From now on, update the modified date of an object even when accessing it
     * in the archive data store. Usually, the modified date is only updated
     * when creating a new object, or when a new link is added to an existing
     * object. When this setting is enabled, even getLength() will update the
     * modified date.
     * 
     * @param before
     *            - update the modified date to the current time if it is older
     *            than this value
     */
    public void updateModifiedDateOnAccess(long before) {
        archiveDataStore.updateModifiedDateOnAccess(before);
    }

    /**
     * Delete objects that have a modified date older than the specified date
     * from the archive data store.
     * 
     * @param min
     *            the minimum time
     * @return the number of data records deleted
     * @throws DataStoreException
     */
    public int deleteAllOlderThan(long min) throws DataStoreException {
        return archiveDataStore.deleteAllOlderThan(min);
    }

    /**
     * Get all identifiers from the archive data store.
     * 
     * @return an iterator over all DataIdentifier objects
     * @throws DataStoreException
     *             if the list could not be read
     */
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return archiveDataStore.getAllIdentifiers();
    }

    public DataRecord getRecordFromReference(String reference)
            throws DataStoreException {
        DataRecord record = primaryDataStore.getRecordFromReference(reference);
        if (record == null) {
            record = archiveDataStore.getRecordFromReference(reference);
        }
        return record;
    }

    /**
     * {@inheritDoc}
     */
    public void init(String homeDir) throws RepositoryException {
        if (delayedDelete) {
            // First initialize the identifiersToDeleteFile
            LocalFileSystem fileSystem = new LocalFileSystem();
            fileSystem.setRoot(new File(homeDir));
            identifiersToDeleteFile = new FileSystemResource(fileSystem, FileSystem.SEPARATOR
                    + IDENTIFIERS_TO_DELETE_FILE_KEY);
        }
        moveDataTaskThread = new Thread(new MoveDataTask(),
                "Jackrabbit-MulitDataStore-MoveDataTaskThread");
        moveDataTaskThread.setDaemon(true);
        moveDataTaskThread.start();
        log.info("MultiDataStore-MoveDataTask thread started; first run scheduled at "
                + moveDataTaskNextRun.getTime());
        if (delayedDelete) {
            try {
                // Run on startup the DeleteDelayedIdentifiersTask only if the
                // file exists and modify date is older than the
                // delayedDeleteSleep timeout ...
                if (identifiersToDeleteFile != null
                        && identifiersToDeleteFile.exists()
                        && (identifiersToDeleteFile.lastModified() + (delayedDeleteSleep * 1000)) < System
                                .currentTimeMillis()) {
                    deleteDelayedIdentifiersTaskThread = new Thread(
                            //Start immediately ...
                            new DeleteDelayedIdentifiersTask(0L),
                            "Jackrabbit-MultiDataStore-DeleteDelayedIdentifiersTaskThread");
                    deleteDelayedIdentifiersTaskThread.setDaemon(true);
                    deleteDelayedIdentifiersTaskThread.start();
                    log.info("Old entries in the " + IDENTIFIERS_TO_DELETE_FILE_KEY
                            + " File found. DeleteDelayedIdentifiersTask-Thread started now.");
                }
            } catch (FileSystemException e) {
                throw new RepositoryException("I/O error while reading from '"
                        + identifiersToDeleteFile.getPath() + "'", e);
            }
        }
    }

    /**
     * Get the minimum size of an object that should be stored in the primary
     * data store.
     * 
     * @return the minimum size in bytes
     */
    public int getMinRecordLength() {
        return primaryDataStore.getMinRecordLength();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws DataStoreException {
        DataStoreException lastException = null;
        // 1. close the primary data store
        try {
            primaryDataStore.close();
        } catch (DataStoreException e) {
            lastException = e;
        }
        // 2. close the archive data store
        try {
            archiveDataStore.close();
        } catch (DataStoreException e) {
            if (lastException != null) {
                lastException = new DataStoreException(lastException);
            }
        }
        // 3. if moveDataTaskThread is running interrupt it
        try {
            if (moveDataTaskRunning) {
                moveDataTaskThread.interrupt();
            }
        } catch (Exception e) {
            if (lastException != null) {
                lastException = new DataStoreException(lastException);
            }
        }
        // 4. if deleteDelayedIdentifiersTaskThread is running interrupt it
        try {
            if (deleteDelayedIdentifiersTaskThread != null
                    && deleteDelayedIdentifiersTaskThread.isAlive()) {
                deleteDelayedIdentifiersTaskThread.interrupt();
            }
        } catch (Exception e) {
            if (lastException != null) {
                lastException = new DataStoreException(lastException);
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearInUse() {
        archiveDataStore.clearInUse();
    }

    public int getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(int maxAge) {
        this.maxAge = maxAge;
    }

    public int getMoveDataTaskSleep() {
        return moveDataTaskSleep;
    }

    public int getMoveDataTaskFirstRunHourOfDay() {
        return moveDataTaskNextRun.get(Calendar.HOUR_OF_DAY);
    }

    public void setMoveDataTaskSleep(int sleep) {
        this.moveDataTaskSleep = sleep;
    }

    public void setMoveDataTaskFirstRunHourOfDay(int hourOfDay) {
        moveDataTaskNextRun = Calendar.getInstance();
        if (moveDataTaskNextRun.get(Calendar.HOUR_OF_DAY) >= hourOfDay) {
            moveDataTaskNextRun.add(Calendar.DAY_OF_MONTH, 1);
        }
        moveDataTaskNextRun.set(Calendar.HOUR_OF_DAY, hourOfDay);
        moveDataTaskNextRun.set(Calendar.MINUTE, 0);
        moveDataTaskNextRun.set(Calendar.SECOND, 0);
        moveDataTaskNextRun.set(Calendar.MILLISECOND, 0);
    }

    public void setSleepBetweenRecords(long millis) {
        this.sleepBetweenRecords = millis;
    }

    public long getSleepBetweenRecords() {
        return sleepBetweenRecords;
    }

    public boolean isDelayedDelete() {
        return delayedDelete;
    }

    public void setDelayedDelete(boolean delayedDelete) {
        this.delayedDelete = delayedDelete;
    }

    public long getDelayedDeleteSleep() {
        return delayedDeleteSleep;
    }

    public void setDelayedDeleteSleep(long delayedDeleteSleep) {
        this.delayedDeleteSleep = delayedDeleteSleep;
    }

    /**
     * Writes the given DataIdentifier to the delayedDeletedFile.
     * 
     * @param identifier
     * @return boolean true if it was successful otherwise false
     */
    private boolean writeDelayedDataIdentifier(DataIdentifier identifier) {
        BufferedWriter writer = null;
        try {
            File identifierFile = new File(
                    ((LocalFileSystem) identifiersToDeleteFile.getFileSystem()).getPath(),
                    identifiersToDeleteFile.getPath());
            writer = new BufferedWriter(new FileWriter(identifierFile, true));
            writer.write(identifier.toString());
            return true;
        } catch (Exception e) {
            log.warn("I/O error while saving DataIdentifier (stacktrace on DEBUG log level) to '"
                    + identifiersToDeleteFile.getPath() + "': " + e.getMessage());
            log.debug("Root cause: ", e);
            return false;
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    /**
     * Purges the delayedDeletedFile.
     * 
     * @return boolean true if it was successful otherwise false
     */
    private boolean purgeDelayedDeleteFile() {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    identifiersToDeleteFile.getOutputStream()));
            writer.write("");
            return true;
        } catch (Exception e) {
            log.warn("I/O error while purging (stacktrace on DEBUG log level) the "
                    + IDENTIFIERS_TO_DELETE_FILE_KEY + " file '"
                    + identifiersToDeleteFile.getPath() + "': " + e.getMessage());
            log.debug("Root cause: ", e);
            return false;
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    /**
     * Class for maintaining the MultiDataStore. It will be used to move the
     * content of the primary data store to the archive data store.
     */
    public class MoveDataTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    log.info("Next move-data task run scheduled at "
                            + moveDataTaskNextRun.getTime());
                    long sleepTime = moveDataTaskNextRun.getTimeInMillis()
                            - System.currentTimeMillis();
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                    moveDataTaskRunning = true;
                    moveOutdatedData();
                    moveDataTaskRunning = false;
                    moveDataTaskNextRun.add(Calendar.SECOND, moveDataTaskSleep);
                    if (delayedDelete) {
                        if (deleteDelayedIdentifiersTaskThread != null
                                && deleteDelayedIdentifiersTaskThread.isAlive()) {
                            log.warn("The DeleteDelayedIdentifiersTask-Thread is already running.");
                        } else {
                            deleteDelayedIdentifiersTaskThread = new Thread(
                                    new DeleteDelayedIdentifiersTask(delayedDeleteSleep),
                                    "Jackrabbit-MultiDataStore-DeleteDelayedIdentifiersTaskThread");
                            deleteDelayedIdentifiersTaskThread.setDaemon(true);
                            deleteDelayedIdentifiersTaskThread.start();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.warn("Interrupted: stopping move-data task.");
        }

        /**
         * Moves outdated data from primary to archive data store
         */
        protected void moveOutdatedData() {
            try {
                long now = System.currentTimeMillis();
                long maxAgeMilli = 1000L * 60 * 60 * 24 * maxAge;
                log.debug("Collecting all Identifiers from PrimaryDataStore...");
                Iterator<DataIdentifier> allIdentifiers = primaryDataStore.getAllIdentifiers();
                int moved = 0;
                while (allIdentifiers.hasNext()) {
                    DataIdentifier identifier = allIdentifiers.next();
                    DataRecord dataRecord = primaryDataStore.getRecord(identifier);
                    if ((dataRecord.getLastModified() + maxAgeMilli) < now) {
                        try {
                            moveDataTaskLock.lock();
                            if (delayedDelete) {
                                // first write it to the file and then add it to
                                // the archive data store ...
                                if (writeDelayedDataIdentifier(identifier)) {
                                    archiveDataStore.addRecord(dataRecord.getStream());
                                    moved++;
                                }
                            } else {
                                // first add it and then delete it .. not really
                                // atomic ...
                                archiveDataStore.addRecord(dataRecord.getStream());
                                ((MultiDataStoreAware) primaryDataStore).deleteRecord(identifier);
                                moved++;
                            }
                            if (moved % 100 == 0) {
                                log.debug("Moving DataRecord's... ({})", moved);
                            }
                        } catch (DataStoreException e) {
                            log.error("Failed to move DataRecord. DataIdentifier: " + identifier, e);
                        } finally {
                            moveDataTaskLock.unlock();
                        }
                    }
                    // Give other threads time to use the MultiDataStore while
                    // MoveDataTask is running..
                    Thread.sleep(sleepBetweenRecords);
                }
                if (delayedDelete) {
                    log.info("Moved "
                            + moved
                            + " DataRecords to the archive data store. The DataRecords in the primary data store will be removed in "
                            + delayedDeleteSleep + " seconds.");
                } else {
                    log.info("Moved " + moved + " DataRecords to the archive data store.");
                }
            } catch (Exception e) {
                log.warn("Failed to run move-data task.", e);
            }
        }
    }

    /**
     * Class to clean up the delayed DataRecords from the primary data store.
     */
    public class DeleteDelayedIdentifiersTask implements Runnable {

        boolean run = true;
        private long sleepTime = 0L;
        
        /**
         * Constructor
         * @param sleep how long this DeleteDelayedIdentifiersTask should sleep in seconds.
         */
        public DeleteDelayedIdentifiersTask(long sleep) {
            this.sleepTime = (sleep * 1000L);
        }

        @Override
        public void run() {
            if (moveDataTaskRunning) {
                log.warn("It's not supported to run the DeleteDelayedIdentifiersTask while the MoveDataTask is running.");
                return;
            }
            while (run && !Thread.currentThread().isInterrupted()) {
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                log.info("Start to delete DataRecords from the primary data store.");
                BufferedReader reader = null;
                ArrayList<DataIdentifier> problemIdentifiers = new ArrayList<DataIdentifier>();
                try {
                    int deleted = 0;
                    reader = new BufferedReader(new InputStreamReader(
                            identifiersToDeleteFile.getInputStream()));
                    while (true) {
                        String s = reader.readLine();
                        if (s == null || s.equals("")) {
                            break;
                        }
                        DataIdentifier identifier = new DataIdentifier(s);
                        try {
                            moveDataTaskLock.lock();
                            ((MultiDataStoreAware) primaryDataStore).deleteRecord(identifier);
                            deleted++;
                        } catch (DataStoreException e) {
                            log.error("Failed to delete DataRecord. DataIdentifier: " + identifier,
                                    e);
                            problemIdentifiers.add(identifier);
                        } finally {
                            moveDataTaskLock.unlock();
                        }
                        // Give other threads time to use the MultiDataStore
                        // while
                        // DeleteDelayedIdentifiersTask is running..
                        Thread.sleep(sleepBetweenRecords);
                    }
                    log.info("Deleted " + deleted + " DataRecords from the primary data store.");
                    if (problemIdentifiers.isEmpty()) {
                        try {
                            identifiersToDeleteFile.delete();
                        } catch (FileSystemException e) {
                            log.warn("Unable to delete the " + IDENTIFIERS_TO_DELETE_FILE_KEY
                                    + " File.");
                            if (!purgeDelayedDeleteFile()) {
                                log.error("Unable to purge the " + IDENTIFIERS_TO_DELETE_FILE_KEY
                                        + " File.");
                            }
                        }
                    } else {
                        if (purgeDelayedDeleteFile()) {
                            for (int x = 0; x < problemIdentifiers.size(); x++) {
                                writeDelayedDataIdentifier(problemIdentifiers.get(x));
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted: stopping delayed-delete task.");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.warn("Failed to run delayed-delete task.", e);
                } finally {
                    IOUtils.closeQuietly(reader);
                    run = false;
                }
            }
        }
    }

}
