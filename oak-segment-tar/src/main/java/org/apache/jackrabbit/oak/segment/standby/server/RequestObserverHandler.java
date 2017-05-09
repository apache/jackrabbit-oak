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

import java.net.InetSocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentRequest;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;

/**
 * Notifies an observer when a valid request has been received and parsed by
 * this server.
 */
class RequestObserverHandler extends ChannelInboundHandlerAdapter {

    private final CommunicationObserver observer;

    RequestObserverHandler(CommunicationObserver observer) {
        this.observer = observer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();

        if (msg instanceof GetHeadRequest) {
            onGetHeadRequest((GetHeadRequest) msg, address);
        } else if (msg instanceof GetSegmentRequest) {
            onGetSegmentRequest((GetSegmentRequest) msg, address);
        } else if (msg instanceof GetBlobRequest) {
            onGetBlobRequest((GetBlobRequest) msg, address);
        }

        ctx.fireChannelRead(msg);
    }

    private void onGetHeadRequest(GetHeadRequest request, InetSocketAddress address) throws Exception {
        observer.gotMessageFrom(request.getClientId(), "get head", address.getAddress().getHostAddress(), address.getPort());
    }

    private void onGetSegmentRequest(GetSegmentRequest request, InetSocketAddress address) throws Exception {
        observer.gotMessageFrom(request.getClientId(), "get segment", address.getAddress().getHostAddress(), address.getPort());
    }

    private void onGetBlobRequest(GetBlobRequest request, InetSocketAddress address) throws Exception {
        observer.gotMessageFrom(request.getClientId(), "get blob id", address.getAddress().getHostAddress(), address.getPort());
    }

}
