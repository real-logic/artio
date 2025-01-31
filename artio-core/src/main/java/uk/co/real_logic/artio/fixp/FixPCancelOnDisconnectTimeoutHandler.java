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
package uk.co.real_logic.artio.fixp;

import uk.co.real_logic.artio.session.CancelOnDisconnectTimeoutHandler;

/**
 * Handler interface that is invoked after a cancel on disconnect timeout for an acceptor session.
 *
 * In order for this handler to be invoked:
 * <ul>
 *  <li>Your FIX session dictionary must contain a cancel on disconnect type field associated with a establish
 *  message.</li>
 *  <li>A session must specify a CancelOnDisconnectType field in it's establish message that requires
 * a cancel on either logout, disconnect or both</li>
 *  <li>the CODTimeoutWindow also specified in the establish message must have expired without a reconnect</li>
 * </ul>.
 *
 * You can see <a href="https://github.com/artiofix/artio/wiki/Cancel-On-Disconnect-Notification">the wiki</a>
 * for more details around Cancel on disconnect support.
 *
 * The FIX equivalent to this interface is {@link CancelOnDisconnectTimeoutHandler}.
 */
public interface FixPCancelOnDisconnectTimeoutHandler
{
    /**
     * Method invoked when a cancel on disconnect is triggered. This handler is invoked on the Framer thread.
     *
     * @param sessionId the surrogate session id of the cod session.
     * @param context the FIXP context of the cod session.
     */
    void onCancelOnDisconnectTimeout(long sessionId, FixPContext context);
}
