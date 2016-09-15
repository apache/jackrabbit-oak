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
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetBlobRequestEncoder extends MessageToMessageEncoder<GetBlobRequest> {

    private final Logger log = LoggerFactory.getLogger(GetBlobRequestEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, GetBlobRequest msg, List<Object> out) throws Exception {
        log.debug("Sending request from client {} for blob {}", msg.getClientId(), msg.getBlobId());
        out.add(Messages.newGetBlobRequest(msg.getClientId(), msg.getBlobId()));
    }

}
