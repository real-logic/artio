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
package uk.co.real_logic.artio.library;

import io.aeron.logbuffer.ControlledFragmentHandler;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.messages.FixPProtocolType;

/**
 * Callback that is invoked when a library is notified of for FIXP connections.
 *
 * This will either be called when a new session is accepted on the gateway or
 * when the library first connects.
 *
 * @see LibraryConfiguration#fixPConnectionExistsHandler(FixPConnectionExistsHandler)
 * @see SessionExistsHandler the FIX equivalent
 */
@FunctionalInterface
public interface FixPConnectionExistsHandler
{
    /**
     * Invoked when a FIXP connection exists.
     *
     * @param library the library that is being notified.
     * @param surrogateSessionId the Artio sessionId value for the session in question. How this is defined is protocol
     *                           dependent. It can be used to request the FIXP connection and refer to it stable-ly.
     * @param protocol the type of protocol that
     * @param context a protocol specific context object that describes the connection. Fields contained within
     *                the context are protocol specific.
     * @return An Aeron Action instance to define backpressure - normally this would be either CONTINUE if things are
     * ok or ABORT if your handler has been backpressured and wants to be retried.
     */
    ControlledFragmentHandler.Action onConnectionExists(
        FixLibrary library,
        long surrogateSessionId,
        FixPProtocolType protocol,
        FixPContext context);
}
