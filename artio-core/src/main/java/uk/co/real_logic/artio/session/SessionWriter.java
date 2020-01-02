/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.session;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.messages.MessageStatus.OK;

/**
 * A writer for a session that sends messages into the system.
 *
 * This can be used to integrate Artio into a clustering system by providing a way to write messages into the FIX log
 * that have been acknowledged by a cluster. In this way a passive or follower FIX Gateway can be kept up to date with
 * the messages from an active or leader Gateway.
 */
public class SessionWriter
{
    private final int libraryId;
    private final long id;
    private final long connectionId;
    private final MutableAsciiBuffer asciiBuffer;
    private final GatewayPublication publication;
    private int sequenceIndex;

    public SessionWriter(
        final int libraryId,
        final long id,
        final long connectionId,
        final MutableAsciiBuffer asciiBuffer,
        final GatewayPublication publication,
        final int sequenceIndex)
    {
        this.libraryId = libraryId;
        this.id = id;
        this.connectionId = connectionId;
        this.asciiBuffer = asciiBuffer;
        this.publication = publication;
        this.sequenceIndex = sequenceIndex;
    }

    public void sequenceIndex(final int sequenceIndex)
    {
        this.sequenceIndex = sequenceIndex;
    }

    /**
     * Send a message on this session.
     *
     * @param encoder the encoder of the message to be sent
     * @param seqNum the sequence number of the sent message
     * @return the position in the stream that corresponds to the end of this message or a negative
     * number indicating an error status.
     * @throws IndexOutOfBoundsException if the encoded message is too large, if this happens consider
     *                                   increasing {@link CommonConfiguration#sessionBufferSize(int)}
     */
    public long send(final Encoder encoder, final int seqNum)
    {
        final long result = encoder.encode(asciiBuffer, 0);
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);

        return send(asciiBuffer, offset, length, seqNum, encoder.messageType());
    }

    /**
     * Send a message on this session.
     *
     * @param messageBuffer the buffer with the FIX message in to send
     * @param offset the offset within the messageBuffer where the message starts
     * @param length the length of the message within the messageBuffer
     * @param seqNum the sequence number of the sent message
     * @param messageType the long encoded message type.
     * @return the position in the stream that corresponds to the end of this message or a negative
     * number indicating an error status.
     */
    public long send(
        final DirectBuffer messageBuffer, final int offset, final int length, final int seqNum, final long messageType)
    {
        return publication.saveMessage(
            messageBuffer, offset, length, libraryId, messageType, id, sequenceIndex, connectionId, OK, seqNum);
    }

    /**
     * Request the TCP disconnect of a session.
     *
     * @param reason the reason to log for the disconnect.
     * @return the position in the stream that corresponds to the end of this message or a negative
     *         number indicating an error status.
     */
    public long requestDisconnect(final DisconnectReason reason)
    {
        return publication.saveRequestDisconnect(libraryId, connectionId, reason);
    }
}
