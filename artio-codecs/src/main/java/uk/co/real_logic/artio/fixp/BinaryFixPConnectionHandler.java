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
package uk.co.real_logic.artio.fixp;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.library.BinaryFixPConnection;
import uk.co.real_logic.artio.library.NotAppliedResponse;
import uk.co.real_logic.artio.messages.DisconnectReason;

import static uk.co.real_logic.artio.library.ILink3ConnectionConfiguration.Builder;

/**
 * This handler should be implemented by anyone using Artio to connect to binary FIXP protocols - such as CME's
 * . Your application code
 * will receive callbacks on these messages in response to business level messages. Artio handles session level
 * messages itself.
 *
 * NB: This is an experimental API and is subject to change or potentially removal.
 */
public interface BinaryFixPConnectionHandler
{
    /**
     * Callback for receiving business messages.
     *
     * @param connection the connection receiving this message
     * @param templateId the templateId of the iLink3 SBE message that you have received.
     * @param buffer the buffer containing the message.
     * @param offset the offset within the buffer at which your message starts.
     * @param blockLength the blockLength of the received message.
     * @param version the sbe version of the protocol.
     * @param possRetrans true of the possRetrans flag is set to true.
     */
    void onBusinessMessage(
        BinaryFixPConnection connection,
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
     * @param connection the connection receiving this message
     * @param fromSequenceNumber the fromSequenceNumber of the NotApplied message.
     * @param msgCount the msgCount of the NotApplied message.
     * @param response used to tell Artio how to respond to the NotApplied message.
     */
    void onNotApplied(
        BinaryFixPConnection connection, long fromSequenceNumber, long msgCount, NotAppliedResponse response);

    /**
     * Callback when Artio has received a RetransmitReject message. This can be used for error logging or handling.
     * @param connection the connection receiving this message
     * @param reason the reason of the RetransmitReject message
     * @param lastUuid the lastUuid of the RetransmitReject message
     * @param requestTimestamp the requestTimestamp of the RetransmitReject message
     * @param errorCodes the errorCodes of the RetransmitReject message
     */
    void onRetransmitReject(
        BinaryFixPConnection connection, String reason, long lastUuid, long requestTimestamp, int errorCodes);

    /**
     * Callback triggered by a timeout on a retransmit request. See
     * {@link Builder#retransmitNotificationTimeoutInMs(int)} for details.
     *
     * @param connection the connection that initiated the retransmit request.
     */
    void onRetransmitTimeout(BinaryFixPConnection connection);

    /**
     * Notifies an application when a sequence message is received. Normally applications would not need to implement
     * this method or take any behaviour in response to a sequence message - Artio itself provides any session level
     * protocol responses. This method just exposes the event to applications for debugging or certification purposes.
     *
     * @param connection the connection receiving this message
     * @param uuid the UUID of the sequence message.
     * @param nextSeqNo the next sequence number contained in the body of the sequence message.
     */
    void onSequence(BinaryFixPConnection connection, long uuid, long nextSeqNo);

    /**
     * Callback when an error happens internally with the processing of a message in iLink3 that can't be handled
     * through normal protocol means.
     *
     * @param connection the connection where this error has occurred.
     * @param ex the exception corresponding to an error
     */
    void onError(BinaryFixPConnection connection, Exception ex);

    /**
     * Call when this connection is disconnected.
     *
     * @param connection the connection that was disconnected.
     * @param reason the reason for the disconnection.
     */
    void onDisconnect(BinaryFixPConnection connection, DisconnectReason reason);
}
