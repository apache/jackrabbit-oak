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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

public class StateConsumerTest {

    @Test
    public void shouldProvideChannelRegisteredState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.channelRegistered(mock(ChannelHandlerContext.class));
        verify(consumer).consumeState("channel registered");
    }

    @Test
    public void shouldPropagateChannelRegisteredEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        handler.channelRegistered(ctx);
        verify(ctx).fireChannelRegistered();
    }

    @Test
    public void shouldProvideChannelUnregisteredState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.channelUnregistered(mock(ChannelHandlerContext.class));
        verify(consumer).consumeState("channel unregistered");
    }

    @Test
    public void shouldPropagateChannelUnregisteredEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        handler.channelUnregistered(ctx);
        verify(ctx).fireChannelUnregistered();
    }

    @Test
    public void shouldProvideChannelActiveState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.channelActive(mock(ChannelHandlerContext.class));
        verify(consumer).consumeState("channel active");
    }

    @Test
    public void shouldPropagateChannelActiveEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        handler.channelActive(ctx);
        verify(ctx).fireChannelActive();
    }

    @Test
    public void shouldProvideChannelInactiveState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.channelInactive(mock(ChannelHandlerContext.class));
        verify(consumer).consumeState("channel inactive");
    }

    @Test
    public void shouldPropagateChannelInactiveEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        handler.channelInactive(ctx);
        verify(ctx).fireChannelInactive();
    }

    @Test
    public void shouldProvideGotMessageState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.channelRead(mock(ChannelHandlerContext.class), new Object());
        verify(consumer).consumeState("got message");
    }

    @Test
    public void shouldPropagateChannelReadEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Object msg = new Object();
        handler.channelRead(ctx, msg);
        verify(ctx).fireChannelRead(msg);
    }

    @Test
    public void shouldProvideExceptionState() throws Exception {
        StateConsumer consumer = mock(StateConsumer.class);
        StateHandler handler = new StateHandler(consumer);
        handler.exceptionCaught(mock(ChannelHandlerContext.class), new Throwable("message"));
        verify(consumer).consumeState("exception occurred: message");
    }

    @Test
    public void shouldPropagateExceptionCaughtEvent() throws Exception {
        StateHandler handler = new StateHandler(mock(StateConsumer.class));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Throwable t = new Throwable();
        handler.exceptionCaught(ctx, t);
        verify(ctx).fireExceptionCaught(t);
    }

}
