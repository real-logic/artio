/*
 * Copyright 2020 Monotonic Limited.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;
import uk.co.real_logic.artio.util.Lazy;

final class LazyILinkMessagePrinter implements ILinkMessageConsumer
{
    private final int inboundStreamId;

    private final Lazy<ILinkMessageConsumer> lazyDelegate = new Lazy<>(this::makePrinter);

    private ILinkMessageConsumer makePrinter()
    {
        return ILinkMessageConsumer.makePrinter(inboundStreamId);
    }

    LazyILinkMessagePrinter(final int inboundStreamId)
    {
        this.inboundStreamId = inboundStreamId;
    }

    public void onBusinessMessage(
        final ILinkMessageDecoder iLinkMessage, final DirectBuffer buffer, final int offset, final Header header)
    {
        lazyDelegate.get().onBusinessMessage(iLinkMessage, buffer, offset, header);
    }
}
