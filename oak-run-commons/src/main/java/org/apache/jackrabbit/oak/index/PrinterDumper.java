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

package org.apache.jackrabbit.oak.index;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;

import static com.google.common.base.Preconditions.checkNotNull;

class PrinterDumper {
    private final File outDir;
    private final String fileName;
    private final boolean dumpToSysOut;
    private final Format format;
    private final InventoryPrinter printer;
    private File outFile;

    public PrinterDumper(File outDir, String fileName, boolean dumpToSysOut, Format format, InventoryPrinter printer) {
        this.outDir = outDir;
        this.fileName = fileName;
        this.dumpToSysOut = dumpToSysOut;
        this.format = format;
        this.printer = printer;
    }

    public void dump() throws IOException {
        try (OutputStream os = newOutput()) {
            OutputStream writerStream = dumpToSysOut ? new TeeOutputStream(os, System.out) : os;
            PrintWriter pw = new PrintWriter(writerStream);
            printer.print(pw, format, false);
            pw.flush();
        }
    }

    public File getOutFile() {
        return checkNotNull(outFile);
    }

    private OutputStream newOutput() throws IOException {
        outFile = new File(outDir, fileName);
        return new BufferedOutputStream(FileUtils.openOutputStream(outFile));
    }
}
