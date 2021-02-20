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
package uk.co.real_logic.artio.library;

/**
 * Represents a Session Connection of the Binary Entrypoint protocol.
 * This is a FIXP session protocol with SBE encoded binary messages. It is very similar to CME's iLink3 protocol.
 * Unlike FIX it possible to have multiple connections open with the same session id.
 */
public abstract class BinaryEntrypointConnection extends BinaryConnection
{
    // -----------------------------------------------
    // Accessors
    // -----------------------------------------------

    public abstract int sessionId();

    public abstract long sessionVerId();

}
