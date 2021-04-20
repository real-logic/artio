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
 * Interface recording information that uniquely identifies a FIXP session. This may just be a session id or it may
 * contain information such as the host and port of the gateway that you're connecting to in the case of an initiator
 * protocol.
 *
 * Implementations must provide appropriate value based hashcode / equals implementation.
 */
public interface FixPKey
{
    /**
     * The used to identify a session that hasn't yet been identified
     */
    int UNK_SESSION = -1;

    /**
     * Gets the protocol type for this key.
     *
     * @return the protocol type for this key.
     */
    FixPProtocolType protocolType();

    /**
     * Gets the long surrogate sessionId value for this FIXP protocol. If there is no appropriate conversion then
     * {@link #UNK_SESSION} will be returned.
     *
     * @return the long surrogate sessionId value for this FIXP protocol
     */
    long sessionIdIfExists();
}
