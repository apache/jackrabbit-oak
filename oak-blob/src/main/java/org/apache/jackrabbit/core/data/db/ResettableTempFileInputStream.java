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
package org.apache.jackrabbit.core.data.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * TempFileInputStream that can be reset in order to allow the
 * ConnectionHelper to retry SQL execution in case of failure.
 */
public class ResettableTempFileInputStream extends TempFileInputStream {

    private final FileChannel fileChannel;
    private long mark = 0;

    public ResettableTempFileInputStream(final File file) throws FileNotFoundException {
        this(new FileInputStream(file), file);
    }

    private ResettableTempFileInputStream(final FileInputStream in, final File file) {
        super(in, file);
        this.fileChannel = in.getChannel();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            mark = fileChannel.position();
        } catch (IOException ex) {
            mark = -1;
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (mark == -1) {
            throw new IOException("Mark failed");
        }
        fileChannel.position(mark);
    }
}
