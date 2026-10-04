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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Minimal SBE-compatible message header encoder/decoder.
 * 
 * <p>Implements the standard SBE MessageHeader structure:
 * - blockLength (2 bytes): Length of the root block
 * - templateId (2 bytes): Template identifier for the message type
 * - schemaId (2 bytes): Schema identifier
 * - version (2 bytes): Schema version
 * 
 * <p>Total: 8 bytes (ENCODED_LENGTH)
 */
public class SimpleMessageHeader {
    
    public static final int ENCODED_LENGTH = 8;
    
    // Template IDs for our message types
    public static final int TEMPLATE_ID_WRITE_PROPOSAL = 100;
    public static final int TEMPLATE_ID_WRITE_BATCH = 106; // Batch of write proposals
    public static final int TEMPLATE_ID_DELETE_PROPOSAL = 101;
    public static final int TEMPLATE_ID_SNAPSHOT = 102;
    public static final int TEMPLATE_ID_GC_PROPOSAL = 103;
    public static final int TEMPLATE_ID_GC_VOTE = 104;
    public static final int TEMPLATE_ID_GC_EXECUTE = 105;
    public static final int TEMPLATE_ID_GENESIS_PROPOSAL = 107; // Genesis creation command
    // ADR 026: Segment persistence acknowledgment flow
    /** Legacy: no longer sent, ignored when an older log is replayed. */
    public static final int TEMPLATE_ID_QUEUE_SEGMENT = 108;
    public static final int TEMPLATE_ID_SEGMENT_PERSISTED = 109;
    /** Legacy: no longer sent, ignored when an older log is replayed. */
    public static final int TEMPLATE_ID_ACK_SEGMENT_PERSISTED = 110;
    // Transaction boundary protocol messages (OakRS parity gap closure)
    public static final int TEMPLATE_ID_START_TRANSACTION = 111;
    public static final int TEMPLATE_ID_COMMIT_TRANSACTION = 112;
    public static final int TEMPLATE_ID_ABORT_TRANSACTION = 113;
    
    // Schema ID and version (arbitrary values for our custom messages)
    private static final int SCHEMA_ID = 1;
    private static final int SCHEMA_VERSION = 1;
    
    /**
     * Encode a message header into the buffer at the given offset.
     * 
     * @param buffer The buffer to encode into
     * @param offset The offset in the buffer
     * @param blockLength The length of the message block (excluding header)
     * @param templateId The template ID for the message type
     * @return The total encoded length (ENCODED_LENGTH)
     */
    public static int encode(MutableDirectBuffer buffer, int offset, int blockLength, int templateId) {
        // blockLength (2 bytes, little-endian)
        buffer.putShort(offset, (short) blockLength, java.nio.ByteOrder.LITTLE_ENDIAN);
        offset += 2;
        
        // templateId (2 bytes, little-endian)
        buffer.putShort(offset, (short) templateId, java.nio.ByteOrder.LITTLE_ENDIAN);
        offset += 2;
        
        // schemaId (2 bytes, little-endian)
        buffer.putShort(offset, (short) SCHEMA_ID, java.nio.ByteOrder.LITTLE_ENDIAN);
        offset += 2;
        
        // version (2 bytes, little-endian)
        buffer.putShort(offset, (short) SCHEMA_VERSION, java.nio.ByteOrder.LITTLE_ENDIAN);
        
        return ENCODED_LENGTH;
    }
    
    /**
     * Decode a message header from the buffer at the given offset.
     * 
     * @param buffer The buffer to decode from
     * @param offset The offset in the buffer
     * @return A HeaderInfo object containing the decoded header fields
     */
    public static HeaderInfo decode(DirectBuffer buffer, int offset) {
        // blockLength (2 bytes, little-endian)
        int blockLength = buffer.getShort(offset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        offset += 2;
        
        // templateId (2 bytes, little-endian)
        int templateId = buffer.getShort(offset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        offset += 2;
        
        // schemaId (2 bytes, little-endian)
        int schemaId = buffer.getShort(offset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        offset += 2;
        
        // version (2 bytes, little-endian)
        int version = buffer.getShort(offset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        
        return new HeaderInfo(blockLength, templateId, schemaId, version);
    }
    
    /**
     * Information extracted from a decoded message header.
     */
    public static class HeaderInfo {
        public final int blockLength;
        public final int templateId;
        public final int schemaId;
        public final int version;
        
        public HeaderInfo(int blockLength, int templateId, int schemaId, int version) {
            this.blockLength = blockLength;
            this.templateId = templateId;
            this.schemaId = schemaId;
            this.version = version;
        }
    }
}
