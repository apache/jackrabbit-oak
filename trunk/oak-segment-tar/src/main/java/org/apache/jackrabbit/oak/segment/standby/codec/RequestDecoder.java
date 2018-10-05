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

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestDecoder extends MessageToMessageDecoder<String> {

    private static final Logger log = LoggerFactory.getLogger(RequestDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) throws Exception {
        String request = Messages.extractMessageFrom(msg);

        if (request == null) {
            log.debug("Received invalid message {}, ignoring", msg);
            ctx.fireChannelRead(msg);
        } else if (request.startsWith(Messages.GET_BLOB)) {
            log.debug("Parsed 'get blob' request");
            out.add(new GetBlobRequest(Messages.extractClientFrom(msg), request.substring(Messages.GET_BLOB.length())));
        } else if (request.equalsIgnoreCase(Messages.GET_HEAD)) {
            log.debug("Parsed 'get head' message");
            out.add(new GetHeadRequest(Messages.extractClientFrom(msg)));
        } else if (request.startsWith(Messages.GET_SEGMENT)) {
            log.debug("Parsed 'get segment' message");
            out.add(new GetSegmentRequest(Messages.extractClientFrom(msg), request.substring(Messages.GET_SEGMENT.length())));
        } else if (request.startsWith(Messages.GET_REFERENCES)) {
            log.debug("Parsed 'get references' message");
            out.add(new GetReferencesRequest(Messages.extractClientFrom(msg), request.substring(Messages.GET_REFERENCES.length())));
        } else {
            log.debug("Received unrecognizable message {}, dropping", msg);
        }
    }

}
