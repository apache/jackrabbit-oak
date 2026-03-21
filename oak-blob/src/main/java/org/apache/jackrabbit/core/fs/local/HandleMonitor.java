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
package org.apache.jackrabbit.core.fs.local;

import org.apache.jackrabbit.util.LazyFileInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

/**
 * This Class implements a very simple open handle monitor for the local
 * file system. This is usefull, if the list of open handles, referenced by
 * an open FileInputStream() should be tracked. This can cause problems on
 * windows filesystems where open files cannot be deleted.
 */
public class HandleMonitor {

    /**
     * The default logger
     */
    private static Logger log = LoggerFactory.getLogger(HandleMonitor.class);

    /**
     * the map of open handles (key=File, value=Handle)
     */
    private HashMap<File, Handle> openHandles = new HashMap<File, Handle>();

    /**
     * Opens a file and returns an InputStream
     *
     * @param file
     * @return
     * @throws FileNotFoundException
     */
    public InputStream open(File file) throws FileNotFoundException {
        Handle handle = getHandle(file);
        InputStream in = handle.open();
        return in;
    }

    /**
     * Checks, if the file is open
     * @param file
     * @return
     */
    public boolean isOpen(File file) {
        return openHandles.containsKey(file);
    }

    /**
     * Closes a file
     * @param file
     */
    private void close(File file) {
        openHandles.remove(file);
    }

    /**
     * Returns the handle for a file.
     * @param file
     * @return
     */
    private Handle getHandle(File file) {
        Handle handle = openHandles.get(file);
        if (handle == null) {
            handle = new Handle(file);
            openHandles.put(file, handle);
        }
        return handle;
    }

    /**
     * Dumps the contents of this monitor
     */
    public void dump() {
        log.info("Number of open files: " + openHandles.size());
        for (File file : openHandles.keySet()) {
            Handle handle = openHandles.get(file);
            handle.dump();
        }
    }

    /**
     * Dumps the information for a file
     * @param file
     */
    public void dump(File file) {
        Handle handle = openHandles.get(file);
        if (handle != null) {
            handle.dump(true);
        }
    }

    /**
     * Class representing all open handles to a file
     */
    private class Handle {

        /**
         * the file of this handle
         */
        private File file;

        /**
         * all open streams of this handle
         */
        private HashSet<Handle.MonitoredInputStream> streams = new HashSet<Handle.MonitoredInputStream>();

        /**
         * Creates a new handle for a file
         * @param file
         */
        private Handle(File file) {
            this.file = file;
        }

        /**
         * opens a stream for this handle
         * @return
         * @throws FileNotFoundException
         */
        private InputStream open() throws FileNotFoundException {
            Handle.MonitoredInputStream in = new Handle.MonitoredInputStream(file);
            streams.add(in);
            return in;
        }

        /**
         * Closes a stream
         * @param in
         */
        private void close(MonitoredInputStream in) {
            streams.remove(in);
            if (streams.isEmpty()) {
                HandleMonitor.this.close(file);
            }
        }

        /**
         * Dumps this handle
         */
        private void dump() {
            dump(false);
        }

        /**
         * Dumps this handle
         */
        private void dump(boolean detailed) {
            if (detailed) {
                log.info("- " + file.getPath() + ", " + streams.size());
                for (Handle.MonitoredInputStream in : streams) {
                    in.dump();
                }
            } else {
                log.info("- " + file.getPath() + ", " + streams.size());
            }
        }

        /**
         * Delegating input stream that registers/unregisters itself from the
         * handle.
         */
        private class MonitoredInputStream extends LazyFileInputStream {

            /**
             * throwable of the time, the stream was created
             */
            private final Throwable throwable = new Exception();

            /**
             * {@inheritDoc}
             */
            private MonitoredInputStream(File file) throws FileNotFoundException {
                super(file);
            }

            /**
             * dumps this stream
             */
            private void dump() {
                log.info("- opened by : ", throwable);
            }

            /**
             * {@inheritDoc}
             */
            public void close() throws IOException {
                // remove myself from the set
                Handle.this.close(this);
                super.close();
            }

        }
    }

}
