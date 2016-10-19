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

package org.apache.jackrabbit.oak.segment.standby.codec;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(ResponseDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int length = in.readInt();

        switch (in.readByte()) {
            case Messages.HEADER_RECORD:
                log.debug("Decoding 'get head' response");
                decodeGetHeadResponse(length, in, out);
                break;
            case Messages.HEADER_SEGMENT:
                log.debug("Decoding 'get segment' response");
                decodeGetSegmentResponse(length, in, out);
                break;
            case Messages.HEADER_BLOB:
                log.debug("Decoding 'get blob' response");
                decodeGetBlobResponse(length, in, out);
                break;
            case Messages.HEADER_REFERENCES:
                log.debug("Decoding 'get references' response");
                decodeGetReferencesResponse(length, in, out);
                break;
            default:
                log.debug("Invalid type, dropping message");
        }
    }

    private void decodeGetHeadResponse(int length, ByteBuf in, List<Object> out) {
        byte[] data = new byte[length - 1];
        in.readBytes(data);
        String recordId = new String(data, Charsets.UTF_8);
        out.add(new GetHeadResponse(null, recordId));
    }

    private void decodeGetSegmentResponse(int length, ByteBuf in, List<Object> out) {
        long msb = in.readLong();
        long lsb = in.readLong();

        String segmentId = new UUID(msb, lsb).toString();

        long hash = in.readLong();

        byte[] data = new byte[length - 25];
        in.readBytes(data);

        if (hash(data) != hash) {
            log.debug("Invalid checksum, discarding segment {}", segmentId);
            return;
        }

        out.add(new GetSegmentResponse(null, segmentId, data));
    }

    private void decodeGetBlobResponse(int length, ByteBuf in, List<Object> out) {
        int blobIdLength = in.readInt();

        byte[] blobIdBytes = new byte[blobIdLength];
        in.readBytes(blobIdBytes);

        String blobId = new String(blobIdBytes, Charsets.UTF_8);

        long hash = in.readLong();

        byte[] blobData = new byte[length - 1 - 4 - blobIdBytes.length - 8];
        in.readBytes(blobData);

        if (hash(blobData) != hash) {
            log.debug("Invalid checksum, discarding blob {}", blobId);
            return;
        }

        out.add(new GetBlobResponse(null, blobId, blobData));
    }

    private void decodeGetReferencesResponse(int length, ByteBuf in, List<Object> out) {
        byte[] data = new byte[length - 1];

        in.readBytes(data);

        String body = new String(data, Charsets.UTF_8);

        int colon = body.indexOf(":");

        if (colon < 0) {
            return;
        }

        String segmentId = body.substring(0, colon);
        String referencesList = body.substring(colon + 1);

        List<String> references;

        if (referencesList.isEmpty()) {
            references = emptyList();
        } else {
            references = asList(referencesList.split(","));
        }

        out.add(new GetReferencesResponse(null, segmentId, references));
    }

    private long hash(byte[] data) {
        return Hashing.murmur3_32().newHasher().putBytes(data).hash().padToLong();
    }

}
