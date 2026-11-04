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
package org.apache.jackrabbit.oak.segment.file.tar;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.zip.CRC32;

import static org.apache.jackrabbit.oak.commons.Buffer.wrap;
import static org.apache.jackrabbit.oak.segment.file.tar.SegmentTarManager.NAME_PATTERN;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;

public final class SegmentTarRecovery {

    private static final Logger log = LoggerFactory.getLogger(SegmentTarRecovery.class);

    /**
     * Scans through the tar file, looking for all segment entries.
     *
     * @param io      The IO access to the TAR file.
     * @param entries The map that will contain the recovered entries. The
     *                entries are inserted in the {@link LinkedHashMap} in the
     *                order they appear in the TAR file.
     */
    public static void recoverEntries(IOReadAccess io, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        byte[] header;
        int fileSize = io.length();
        int currentPosition = 0;
        while (currentPosition + BLOCK_SIZE <= fileSize) {
            // read the tar header block
            header = io.read(currentPosition, BLOCK_SIZE).array();
            currentPosition += BLOCK_SIZE;

            // compute the header checksum
            int sum = 0;
            for (int i = 0; i < BLOCK_SIZE; i++) {
                sum += header[i] & 0xff;
            }


            // identify possible zero block
            if (sum == 0 && currentPosition + 2 * BLOCK_SIZE == fileSize) {
                return; // found the zero blocks at the end of the file
            }

            // replace the actual stored checksum with spaces for comparison
            for (int i = 148; i < 148 + 8; i++) {
                sum -= header[i] & 0xff;
                sum += ' ';
            }

            byte[] checkbytes = String.format("%06o\0 ", sum).getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < checkbytes.length; i++) {
                if (checkbytes[i] != header[148 + i]) {
                    log.warn("Invalid entry checksum at offset {} in tar file {}, skipping...",
                            currentPosition - BLOCK_SIZE, io.getName());
                }
            }

            // The header checksum passes, so read the entry name and size
            Buffer buffer = wrap(header);
            String name = readString(buffer, 100);
            buffer.position(124);
            int size = readNumber(buffer, 12);
            if (currentPosition + size > fileSize) {
                // checksum was correct, so the size field should be accurate
                log.warn("Partial entry {} in tar file {}, ignoring...", name, io.getName());
                return;
            }

            Matcher matcher = NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                UUID id = UUID.fromString(matcher.group(1));

                String checksum = matcher.group(3);
                if (checksum != null || !entries.containsKey(id)) {
                    byte[] data = io.read(currentPosition, size).array();
                    currentPosition += size;

                    // skip possible padding to stay at block boundaries
                    int remainder = currentPosition % BLOCK_SIZE;
                    if (remainder != 0) {
                        currentPosition += (BLOCK_SIZE - remainder);
                    }

                    if (checksum != null) {
                        CRC32 crc = new CRC32();
                        crc.update(data, 0, data.length);
                        if (crc.getValue() != Long.parseLong(checksum, 16)) {
                            log.warn("Checksum mismatch in entry {} of tar file {}, skipping...",
                                    name, io.getName());
                            continue;
                        }
                    }

                    entries.put(id, data);
                }
            } else if (!name.equals(io.getName() + ".idx")) {
                log.warn("Unexpected entry {} in tar file {}, skipping...",
                        name, io.getName());
                int position = currentPosition + size;
                int remainder = position % BLOCK_SIZE;
                if (remainder != 0) {
                    position += BLOCK_SIZE - remainder;
                }
                currentPosition = position;
            }
        }
    }

    private static String readString(Buffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int n = 0;
        while (n < fieldSize && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, StandardCharsets.UTF_8);
    }

    private static int readNumber(Buffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int number = 0;
        for (int i = 0; i < fieldSize; i++) {
            int digit = b[i] & 0xff;
            if ('0' <= digit && digit <= '7') {
                number = number * 8 + digit - '0';
            } else {
                break;
            }
        }
        return number;
    }
}
