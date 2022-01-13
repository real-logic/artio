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

import uk.co.real_logic.artio.fixp.FixPConnection;

/**
 * Represents a Session Connection of the iLink3 protocol.
 * This is a FIXP session protocol with SBE encoded binary messages. Unlike FIX it possible to have multiple connections
 * open with the same session id.
 */
public interface ILink3Connection extends FixPConnection
{
    // -----------------------------------------------
    // Operations
    // -----------------------------------------------

    /**
     * Initiate a termination. This sends a Terminate message to initiate the termination. Artio's session will await
     * an acknowledging Terminate message from the exchange. If keepAliveInterval elapses without a reply then a TCP
     * disconnect will happen.
     *
     * @param shutdown the shutdown text to send in the Terminate message
     * @param errorCodes the error codes to send in the Terminate message
     */
    void terminate(String shutdown, int errorCodes);

    // -----------------------------------------------
    // Accessors
    // -----------------------------------------------

    /**
     * Gets the UUID of the current connection for this session.
     *
     * @return the UUID of the current connection for this session.
     */
    long uuid();

    /**
     * Gets the UUID of the last success connection for this session.
     *
     * @return the UUID of the last success connection for this session.
     */
    long lastUuid();

    /**
     * Send a custom retransmit request.
     *
     * @param uuid the UUID of the connection to request a retransmit request. This doesn't necessarily have to be the
     *             current UUID, but it does have to be one for the same session on the same market segment.
     * @param fromSeqNo the sequence number to start from.
     * @param msgCount the number of messages to request a retransmit of.
     * @return the position in the stream that corresponds to the end of this message or a negative
     * number indicating an error status.
     */
    long tryRetransmitRequest(long uuid, long fromSeqNo, int msgCount);

    /**
     * Gets the next received sequence number that will fill the current retransmit request. If there is no
     * retransmit operation in process NOT_AWAITING_RETRANSMIT will be returned.
     *
     * @return the next received sequence number that will fill the current retransmit request.
     */
    long retransmitFillSeqNo();

    /**
     * Gets the next sequence number that Artio expects to received in the current retransmit request. If there is no
     * retransmit operation in process NOT_AWAITING_RETRANSMIT will be returned.
     *
     * @return the next sequence number that Artio expects to received in the current retransmit request.
     */
    long nextRetransmitSeqNo();
}
