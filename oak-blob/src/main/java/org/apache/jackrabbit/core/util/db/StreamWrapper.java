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
package org.apache.jackrabbit.core.util.db;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.jackrabbit.core.util.db.ConnectionHelper.RetryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWrapper {

    private final Logger log = LoggerFactory.getLogger(StreamWrapper.class);

    private MarkDetectingInputStream stream;
    private final long size;

    /**
     * Creates a wrapper for the given InputStream that can
     * safely be passed as a parameter to the {@link ConnectionHelper#exec(String, Object...)},
     * {@link ConnectionHelper#exec(String, Object[], boolean, int)} and
     * {@link ConnectionHelper#update(String, Object[])} methods.
     *
     * @param in the InputStream to wrap
     * @param size the size of the input stream
     */
    public StreamWrapper(InputStream in, long size) {
        this.stream = new MarkDetectingInputStream(in);
        this.size = size;
    }

    public InputStream getStream() {
        return new CloseShieldInputStream(stream);
    }

    public long getSize() {
        return size;
    }

    public void closeStream() {
        try {
            stream.close();
        } catch (IOException e) {
            log.error("Error while closing stream", e);
        }
    }

    /**
     * Resets the internal InputStream that it could be re-read.<br>
     * Is used from {@link ConnectionHelper.RetryManager} if a {@link SQLException} has occurred.<br>
     * It relies on the assumption that the InputStream was not marked anywhere
     * during reading.
     *
     * @return returns true if it was able to reset the Stream
     */
    public boolean resetStream() {
        try {
            if (!stream.isMarked()) {
                stream.reset();
                return true;
            } else {
                log.warn("Cannot reset stream to the beginning because it was marked.");
                return false;
            }
        } catch (IOException e) {
            return false;
        }
	}

    private static class MarkDetectingInputStream extends FilterInputStream {

        private boolean marked;

        protected MarkDetectingInputStream(final InputStream in) {
            super(in);
        }

        @Override
        public synchronized void mark(final int readlimit) {
            super.mark(readlimit);
            marked = true;
        }

        private boolean isMarked() {
            return marked;
        }
    }
}
