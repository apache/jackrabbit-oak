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

package org.apache.jackrabbit.oak.segment.standby.server;

import java.net.SocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler that filters out incoming connections based on the address of the
 * client. This handler immediately closes the incoming connection if an
 * unwanted client is detected.
 */
class ClientFilterHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ClientFilterHandler.class);

    private final ClientFilter filter;

    /**
     * Create a new handler based on the provided filtering strategy.
     *
     * @param filter an instance of {@link ClientFilter} representing the
     *               filtering strategy for this handler.
     */
    ClientFilterHandler(ClientFilter filter) {
        this.filter = filter;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        SocketAddress address = ctx.channel().remoteAddress();

        if (filter.isAllowed(address)) {
            log.debug("Client {} is allowed", address);
            ctx.fireChannelActive();
        } else {
            log.debug("Client {} is rejected", address);
            ctx.close();
        }
    }

}
