/*
 * Copyright 2022 Monotonic Ltd.
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

/**
 * Implementations of {@link FixPContext} should also implement this interface. It contains methods
 * that are called by the FIXP implementation within Artio but not to be used by API users.
 */
public interface InternalFixPContext extends FixPContext
{
    /**
     * Invoked when an acceptor reconnects.
     *
     * @param oldContext the FixPContext from the previous connect
     * @param ignoreFromNegotiate do not perform validation about whether this is a negotiate or establish message
     * @return a response that might be a reason to reject this connection or OK if there is no error.
     */
    FixPFirstMessageResponse checkAccept(FixPContext oldContext, boolean ignoreFromNegotiate);

    /**
     * Invoked when a sequence is ended. There are two cases for that:
     *
     * <ul>
     *     <li>When the <code>FixEngine.resetSequenceNumber(sessionId)</code> method is invoked.</li>
     *     <li>For FIXP protocols that implement the FinishedSending / FinishedReceiving mechanism, it is invoked
     *     upon receipt of either of those methods.</li>
     * </ul>
     *
     * Artio users should never need to call this method. Implementations should be idempotent.
     */
    void onEndSequence();

    /**
     * Invoked when an initiator reconnects.
     *
     * @param reestablishConnection true if the initiator's configuration for reestablishing the connection has set
     *                              this value to true.
     * @throws UnsupportedOperationException if protocol not implemented as an initiator
     */
    void initiatorReconnect(boolean reestablishConnection);

    /**
     * Invoked when an initiator receives a response to its negotiate.
     *
     * @return true iff this is a new context, false if it's an update
     */
    boolean onInitiatorNegotiateResponse();

    void onInitiatorDisconnect();

    long surrogateSessionId();

    boolean hasUnsentMessagesAtNegotiate();
}
