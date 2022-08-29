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

import uk.co.real_logic.artio.messages.FixPProtocolType;

/**
 * Interface recording information about a FIXP connection that is associated with a session. For example
 * session id, version id, connect timestamps.
 */
public interface FixPContext
{
    /**
     * Gets the key that uniquely identifies the session that is associated with this connection. The information within
     * this key differs from protocol to protocol.
     *
     * @return the key that uniquely identifies the session that is associated with this connection.
     */
    FixPKey key();

    /**
     * Gets the protocol type for this key.
     *
     * @return the protocol type for this key.
     */
    FixPProtocolType protocolType();

    int compareVersion(FixPContext oldContext);

    /**
     * Gets whether this context was generated from a <code>Negotiate</code> or an <code>Establish</code> message.
     *
     * @return true iff this context was generated from a <code>Negotiate</code> message.
     */
    boolean fromNegotiate();
}
