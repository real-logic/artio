/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Mockable class for intercepting network communications
 */
public abstract class TcpChannelSupplier implements AutoCloseable
{
    public abstract void open(InetSocketAddress address, InitiatedChannelHandler channelHandler) throws IOException;

    public abstract void stopConnecting(InetSocketAddress address) throws IOException;

    public abstract int pollSelector(long timeInMs, NewChannelHandler handler) throws IOException;

    public abstract void unbind() throws IOException;

    public abstract void bind() throws IOException;

    @FunctionalInterface
    public interface InitiatedChannelHandler
    {
        void onInitiatedChannel(TcpChannel socketChannel, IOException exception);
    }

    @FunctionalInterface
    public interface NewChannelHandler
    {
        void onNewChannel(long timeInMs, TcpChannel socketChannel) throws IOException;
    }
}
