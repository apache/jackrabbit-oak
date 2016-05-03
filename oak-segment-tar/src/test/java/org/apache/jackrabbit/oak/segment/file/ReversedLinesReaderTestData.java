/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Test checks symmetric behaviour with  BufferedReader
 * FIXME: this is mostly taken from a copy of org.apache.commons.io.input
 * with a fix for IO-471. Replace again once commons-io has released a fixed version.
 */
public final class ReversedLinesReaderTestData {
    private ReversedLinesReaderTestData() {}

    public static final byte[] WINDOWS_31J_BIN = new byte[]{
            -126, -97, -126, -96, -126, -95, -126, -94, -126, -93, 13, 10, -106, -66, -105, 65, -114,
            113, -117, -98, 13, 10,
    };

    public static final byte[] GBK_BIN = new byte[]{
            -61, -9, -35, -108, -41, -45, -66, -87, 13, 10, -68, -14, -52, -27, -42, -48, -50, -60,
            13, 10,
    };

    public static final byte[] X_WINDOWS_949_BIN = new byte[]{
            -57, -47, -79, -71, -66, -18, 13, 10, -76, -21, -57, -47, -71, -50, -79, -71, 13, 10,
    };

    public static final byte[] X_WINDOWS_950_BIN = new byte[]{
            -87, -6, -65, -23, -92, 108, -88, -54, 13, 10, -63, 99, -59, -23, -92, -92, -92, -27,
            13, 10,
    };

    public static File createFile(File file, byte[] data) throws IOException {
        FileOutputStream os = new FileOutputStream(file);
        try {
            os.write(data);
            return file;
        } finally {
            os.close();
        }
    }

}
