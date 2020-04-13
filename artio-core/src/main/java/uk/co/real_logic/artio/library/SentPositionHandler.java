/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.library;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;

/**
 * Callback handler to let clients know when the gateway owns a message. In a clustered
 * configuration this means that the gateway has replicated the message around the cluster.
 * In a single node this means that the message is in a buffer on that node waiting to be sent.
 *
 * This can be correlated against the position returned by the session's send method.
 *
 * @see uk.co.real_logic.artio.session.Session#trySend(uk.co.real_logic.artio.builder.Encoder)
 */
@FunctionalInterface
public interface SentPositionHandler
{
    /**
     * Called when one or more messages has been sent.
     *
     * @param position the position that corresponds to what is owned by the gateway.
     * @return appropriate action to indicate back pressure
     */
    Action onSendCompleted(long position);
}
