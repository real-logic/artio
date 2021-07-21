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
package uk.co.real_logic.artio.binary_entrypoint;

import b3.entrypoint.fixp.sbe.CancelOnDisconnectType;
import b3.entrypoint.fixp.sbe.TerminationCode;
import uk.co.real_logic.artio.fixp.FixPConnection;

/**
 * Represents a Session Connection of the Binary Entrypoint protocol.
 * This is a FIXP session protocol with SBE encoded binary messages. It is very similar to CME's iLink3 protocol.
 */
public interface BinaryEntrypointConnection extends FixPConnection
{
    // -----------------------------------------------
    // Operations
    // -----------------------------------------------

    long terminate(TerminationCode terminationCode);

    // -----------------------------------------------
    // Accessors
    // -----------------------------------------------

    long sessionId();

    long sessionVerId();

    void finishSending();

    BinaryEntryPointKey key();

    CancelOnDisconnectType cancelOnDisconnectType();

    long codTimeoutWindow();
}
