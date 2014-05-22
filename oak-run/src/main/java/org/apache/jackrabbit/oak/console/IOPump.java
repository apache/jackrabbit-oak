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
package org.apache.jackrabbit.oak.console;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import com.google.common.io.ByteStreams;

/**
* Copies bytes from the input to the output stream and closes both of them.
*/
final class IOPump {

    private final Thread t;

    private IOPump(final InputStream in, final OutputStream out) {
        this.t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ByteStreams.copy(in, out);
                } catch (Exception e) {
                    // ignore
                }
                IOUtils.closeQuietly(in);
                IOUtils.closeQuietly(out);
            }
        }, "IO Pump Thread");
        t.setDaemon(true);
    }

    static IOPump start(InputStream in, OutputStream out) {
        IOPump p = new IOPump(in, out);
        p.t.start();
        return p;
    }

    void join() throws InterruptedException {
        t.join();
    }
}
