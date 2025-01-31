/*
 * Copyright 2021 Monotonic Ltd.
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

/**
 * Handler interface that is invoked after a cancel on disconnect timeout for an acceptor session.
 *
 * In order for this handler to be invoked:
 * <ul>
 *  <li>The FIX session must have a cancel on disconnect type configured that requires a cancel on either logout,
 *  disconnect or both. This may be be provided either via the logon message or the acceptor engine configuration</li>
 *  <li>The CODTimeoutWindow also specified in the logon message or acceptor engine defaults must have expired without
 *  a reconnect</li>
 * </ul>.
 *
 * To provide these values on the logon message your FIX session dictionary must contain a CancelOnDisconnectType
 * field and optionally a CODTimeoutWindow field. Each of these configuration options are checked by first taking
 * the value provided in the logon message if it is specified and otherwise taking it from the acceptor engine
 * configuration.
 *
 * You can see <a href="https://github.com/artiofix/artio/wiki/Cancel-On-Disconnect-Notification">the wiki</a>
 * for more details around Cancel on disconnect support.
 *
 * Initiator implementations using cancel on disconnect can set the requisite logon fields using a
 * {@link SessionCustomisationStrategy}.
 *
 * The FIXP equivalent to this interface is {@link uk.co.real_logic.artio.fixp.FixPCancelOnDisconnectTimeoutHandler}.
 */
public interface CancelOnDisconnectTimeoutHandler
{
    /**
     * Method invoked when a cancel on disconnect is triggered. This handler is invoked on the Framer thread.
     *
     * @param sessionId the surrogate session id of the cod session.
     * @param fixSessionKey the full session id of the cod session.
     */
    void onCancelOnDisconnectTimeout(long sessionId, CompositeKey fixSessionKey);
}
