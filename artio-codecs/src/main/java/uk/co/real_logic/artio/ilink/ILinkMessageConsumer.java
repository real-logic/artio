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
package uk.co.real_logic.artio.ilink;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;

import java.lang.reflect.InvocationTargetException;

/**
 * Consumer to read iLink3 messages from the archive.
 */
@FunctionalInterface
public interface ILinkMessageConsumer
{
    /**
     * Callback for receiving iLink3 business messages. Details of business messages can be found in the
     * <a href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/iLink+3+Application+Layer">CME
     * Documentation</a>. These may also be referred to as application layer messages.
     *  @param buffer the buffer containing the message.
     * @param offset the offset within the buffer at which your message starts.
     * @param header the Aeron header value for the fragment
     */
    void onBusinessMessage(DirectBuffer buffer, int offset, Header header);

    static ILinkMessageConsumer makePrinter(final int inboundStreamId)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.ilink.PrintingILinkMessageConsumer");
            return (ILinkMessageConsumer)cls.getConstructor(int.class).newInstance(inboundStreamId);
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException | InvocationTargetException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }
}
