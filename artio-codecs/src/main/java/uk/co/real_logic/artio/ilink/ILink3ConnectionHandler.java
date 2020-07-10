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

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.library.NotAppliedResponse;

/**
 * This handler should be implemented by anyone using Artio to connect to the iLink3 protocol. Your application code
 * will receive callbacks on these messages in response to business level messages. Artio handles session level
 * messages itself.
 *
 * NB: This is an experimental API and is subject to change or potentially removal.
 */
public interface ILink3ConnectionHandler
{
    /**
     * Callback for receiving iLink3 business messages. Details of business messages can be found in the
     * <a href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/iLink+3+Application+Layer">CME
     * Documentation</a>. These may also be referred to as application layer messages.
     *
     * @param templateId the templateId of the iLink3 SBE message that you have received.
     * @param buffer the buffer containing the message.
     * @param offset the offset within the buffer at which your message starts.
     * @param blockLength the blockLength of the received message.
     * @param version the sbe version of the protocol.
     * @param possRetrans true of the possRetrans flag is set to true.
     */
    void onBusinessMessage(
        int templateId,
        DirectBuffer buffer,
        int offset,
        int blockLength,
        int version,
        boolean possRetrans);

    /**
     * Callback when Artio has received a NotApplied message. The {@link NotAppliedResponse} parameter can be used
     * in order to get Artio to retransmit messages or use a Sequence message in order to fill the gap.
     *
     * @param fromSequenceNumber the fromSequenceNumber of the NotApplied message.
     * @param msgCount the msgCount of the NotApplied message.
     * @param response used to tell Artio how to respond to the NotApplied message.
     */
    void onNotApplied(long fromSequenceNumber, long msgCount, NotAppliedResponse response);

    /**
     * Callback when Artio has received a RetransmitReject message. This can be used for error logging or handling.
     * @param reason the reason of the RetransmitReject message
     * @param lastUuid the lastUuid of the RetransmitReject message
     * @param requestTimestamp the requestTimestamp of the RetransmitReject message
     * @param errorCodes the errorCodes of the RetransmitReject message
     */
    void onRetransmitReject(String reason, long lastUuid, long requestTimestamp, int errorCodes);

    /**
     * Notifies an application when a sequence message is received. Normally applications would not need to implement
     * this method or take any behaviour in response to a sequence message - Artio itself provides any session level
     * protocol responses. This method just exposes the event to applications for debugging or certification purposes.
     *
     * @param uuid the UUID of the sequence message.
     * @param nextSeqNo the next sequence number contained in the body of the sequence message.
     */
    void onSequence(long uuid, long nextSeqNo);

    /**
     * Callback when an error happens internally with the processing of a message in iLink3 that can't be handled
     * through normal protocol means.
     *
     * @param ex the exception corresponding to an error
     */
    void onError(Exception ex);

    /**
     * Call when this connection is disconnected.
     */
    void onDisconnect();
}
