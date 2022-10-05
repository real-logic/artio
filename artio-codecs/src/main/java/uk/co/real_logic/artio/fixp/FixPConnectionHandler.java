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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.library.NotAppliedResponse;
import uk.co.real_logic.artio.messages.DisconnectReason;

import static uk.co.real_logic.artio.ilink.ILink3ConnectionConfiguration.Builder;

/**
 * This handler should be implemented by anyone using Artio to connect to binary FIXP protocols - such as CME's
 * . Your application code
 * will receive callbacks on these messages in response to business level messages. Artio handles session level
 * messages itself.
 *
 * NB: This is an experimental API and is subject to change or potentially removal.
 */
public interface FixPConnectionHandler
{
    /**
     * Callback for receiving business messages.
     *
     * Note that the offset provided here is for the SBE message itself and doesn't include the header, but a user
     * of this API can always assume that the header of the message exists before the message itself. So for example
     * if you want to take the offset and length of the SBE message, including its MessageHeader and Simple Open
     * Framing Header then you could do the following calculation on the offset and length parameters.
     *
     * <code>
     *     final int encodedLength = b3.entrypoint.fixp.sbe.MessageHeaderDecoder.ENCODED_LENGTH +
     *         uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;
     *     offset -= encodedLength;
     *     length += encodedLength;
     * </code>
     *
     * @param connection the connection receiving this message
     * @param templateId the templateId of the SBE message that you have received.
     * @param buffer the buffer containing the message.
     * @param offset the offset within the buffer at which your message starts.
     * @param blockLength the blockLength of the received message.
     * @param version the sbe version of the protocol.
     * @param possRetrans true of the possRetrans flag is set to true.
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onBusinessMessage(
        FixPConnection connection,
        int templateId,
        DirectBuffer buffer,
        int offset,
        int blockLength,
        int version,
        boolean possRetrans);

    /**
     * Callback when Artio has received a NotApplied message. The {@link NotAppliedResponse} parameter can be used
     * in order to get Artio to retransmit messages or use a Sequence message in order to fill the gap.
     *  @param connection the connection receiving this message
     * @param fromSequenceNumber the fromSequenceNumber of the NotApplied message.
     * @param msgCount the msgCount of the NotApplied message.
     * @param response used to tell Artio how to respond to the NotApplied message.
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onNotApplied(
        FixPConnection connection, long fromSequenceNumber, long msgCount, NotAppliedResponse response);

    /**
     * Callback when Artio has received a RetransmitReject message. This can be used for error logging or handling.
     * @param connection the connection receiving this message
     * @param reason the reason of the RetransmitReject message
     * @param requestTimestamp the requestTimestamp of the RetransmitReject message
     * @param errorCodes the errorCodes of the RetransmitReject message
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onRetransmitReject(
        FixPConnection connection, String reason, long requestTimestamp, int errorCodes);

    /**
     * Callback when Artio has received a RetransmitRequest message. This can be used to reject retransmission
     * attempts or to log errored requests.
     *
     * @param connection the connection receiving this message.
     * @param retransmissionInfo provides information about the retransmission message and how to reject them.
     * @return an action to indicate the correct back pressure behaviour.
     */
    default Action onRetransmitRequest(
        final FixPConnection connection, final RetransmissionInfo retransmissionInfo)
    {
        // default for backwards compatibility.
        return Action.CONTINUE;
    }

    /**
     * Callback triggered by a timeout on a retransmit request. See
     * {@link Builder#retransmitNotificationTimeoutInMs(int)} for details.
     *
     * @param connection the connection that initiated the retransmit request.
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onRetransmitTimeout(FixPConnection connection);

    /**
     * Notifies an application when a sequence message is received. Normally applications would not need to implement
     * this method or take any behaviour in response to a sequence message - Artio itself provides any session level
     * protocol responses. This method just exposes the event to applications for debugging or certification purposes.
     * @param connection the connection receiving this message
     * @param nextSeqNo the next sequence number contained in the body of the sequence message.
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onSequence(FixPConnection connection, long nextSeqNo);

    /**
     * Callback when an error happens internally with the processing of a message in iLink3 that can't be handled
     * through normal protocol means.
     *
     * @param connection the connection where this error has occurred.
     * @param ex the exception corresponding to an error
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onError(FixPConnection connection, Exception ex);

    /**
     * Callback invoked when this connection is disconnected.
     *
     * @param connection the connection that was disconnected.
     * @param reason the reason for the disconnection.
     * @return an action to indicate the correct back pressure behaviour.
     */
    Action onDisconnect(FixPConnection connection, DisconnectReason reason);

    /**
     * Callback invoked when this connection receives a FinishedSending message from a counter-party.
     *
     * No more business messages will be received from the counter-party after this point. If your application
     * has no more messages to send then it should invoke <code>connection.finishedSending()</code> unless it has
     * already called <code>finishedSending()</code> to start the process. Your application may continue to send
     * messages until it has invoked that method.
     *
     * NB: implementing this method is strictly optional. Not all FIXP protocols (eg: iLink3) implement the finished
     * sending / finished receiving mechanism for protocol finalization and this method won't be invoked in the case
     * of those protocols.
     *
     * @param connection the connection that has received the FinishedSending message
     * @return an action to indicate the correct back pressure behaviour.
     */
    default Action onFinishedSending(final FixPConnection connection)
    {
        // Deliberately empty as it's optional for a FIXP protocol to implement this method.
        return Action.CONTINUE;
    }
}
