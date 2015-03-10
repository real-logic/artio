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
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;

import java.util.stream.IntStream;

/**
 * Configuration that exists for the entire duration of a fix gateway
 */
public final class StaticConfiguration
{
    private final Int2ObjectHashMap<OtfMessageAcceptor> otfAcceptors = new Int2ObjectHashMap<>();

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

}
