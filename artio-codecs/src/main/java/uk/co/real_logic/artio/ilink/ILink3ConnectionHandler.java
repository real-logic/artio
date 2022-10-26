/*
 * Copyright 2020 Monotonic Ltd.
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

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fixp.FixPConnectionHandler;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.fixp.FixPMessageHeader;

/**
 * This handler should be implemented by anyone using Artio to connect to the iLink3 protocol. Your application code
 * will receive callbacks on these messages in response to business level messages. Artio handles session level
 * messages itself.
 *
 * NB: This is an experimental API and is subject to change or potentially removal.
 */
public interface ILink3ConnectionHandler extends FixPConnectionHandler
{
    /**
     * Callback for receiving iLink3 business messages. Details of business messages can be found in the
     * <a href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/iLink+3+Application+Layer">CME
     * Documentation</a>. These may also be referred to as application layer messages.
     *  @param connection the connection receiving this message
     * @param templateId the templateId of the iLink3 SBE message that you have received.
     * @param buffer the buffer containing the message.
     * @param offset the offset within the buffer at which your message starts.
     * @param blockLength the blockLength of the received message.
     * @param version the sbe version of the protocol.
     * @param possRetrans true of the possRetrans flag is set to true.
     * @param messageHeader additional fields related to the message.FakeILink3ConnectionHandler
     * @return the action to perform
     */
    @Override
    ControlledFragmentHandler.Action onBusinessMessage(
        FixPConnection connection,
        int templateId,
        DirectBuffer buffer,
        int offset,
        int blockLength,
        int version,
        boolean possRetrans,
        FixPMessageHeader messageHeader);
}
