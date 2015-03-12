/*
 * Copyright 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.fix_gateway.flyweight_api.OrderSingleAcceptor;
import uk.co.real_logic.fix_gateway.framer.session.HashingSenderAndTargetSessionIdStrategy;
import uk.co.real_logic.fix_gateway.framer.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;

import java.net.InetSocketAddress;
import java.util.stream.IntStream;

/**
 * Configuration that exists for the entire duration of a fix gateway
 */
public final class StaticConfiguration
{
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 10;
    private static final int DEFAULT_RECEIVER_BUFFER_SIZE = 8 * 1024;
    private static final long DEFAULT_CONNECTION_TIMEOUT = 1000;
    private static final int DEFAULT_ENCODER_BUFFER_SIZE = 8 * 1024;

    private final Int2ObjectHashMap<OtfMessageAcceptor> otfAcceptors = new Int2ObjectHashMap<>();

    private int defaultHeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int receiverBufferSize = DEFAULT_RECEIVER_BUFFER_SIZE;
    private long connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    private int encoderBufferSize = DEFAULT_ENCODER_BUFFER_SIZE;
    private SessionIdStrategy sessionIdStrategy = new HashingSenderAndTargetSessionIdStrategy();

    private String host;
    private int port;
    private OtfMessageAcceptor fallbackAcceptor;

    public void registerAcceptor(final OrderSingleAcceptor orderSingleAcceptor, final ErrorAcceptor errorAcceptor)
    {
    }

    public void registerAcceptor(final uk.co.real_logic.fix_gateway.reactive_api.OrderSingleAcceptor orderSingleAcceptor)
    {
    }

    public StaticConfiguration registerAcceptor(
        final OtfMessageAcceptor messageAcceptor, int firstTag, final int... tags)
    {
        otfAcceptors.put(firstTag, messageAcceptor);
        IntStream.of(tags).forEach(tag -> otfAcceptors.put(tag, messageAcceptor));
        return this;
    }

    public StaticConfiguration registerFallbackAcceptor(
            final OtfMessageAcceptor fallbackAcceptor)
    {
        this.fallbackAcceptor = fallbackAcceptor;
        return this;
    }

    public StaticConfiguration bind(final String host, final int port)
    {
        this.host = host;
        this.port = port;
        return this;
    }

    /**
     * The default interval for heartbeats if not exchanged upon logon. Specified in seconds.
     *
     * @return this
     */
    public StaticConfiguration defaultHeartbeatInterval(final int value)
    {
        defaultHeartbeatInterval = value;
        return this;
    }

    public StaticConfiguration receiverBufferSize(final int value)
    {
        receiverBufferSize = value;
        return this;
    }

    public StaticConfiguration sessionIdStrategy(final SessionIdStrategy sessionIdStrategy)
    {
        this.sessionIdStrategy = sessionIdStrategy;
        return this;
    }

    int defaultHeartbeatInterval()
    {
        return defaultHeartbeatInterval;
    }

    SessionIdStrategy sessionIdStrategy()
    {
        return sessionIdStrategy;
    }

    int receiverBufferSize()
    {
        return receiverBufferSize;
    }

    InetSocketAddress bindAddress()
    {
        return new InetSocketAddress(host, port);
    }

    long connectionTimeout()
    {
        return connectionTimeout;
    }

    int encoderBufferSize()
    {
        return encoderBufferSize;
    }
}
