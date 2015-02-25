/*
 * Copyright 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.framer;

/**
 * <h1>Transitions</h1>
 *
 * Successful Login: CONNECTED -> ACTIVE
 * Login with high sequence number: CONNECTED -> AWAITING_RESEND
 * Login with low sequence number: CONNECTED -> DISCONNECTED
 * Login with wrong credentials: CONNECTED -> DISCONNECTED or CONNECTED -> DISABLED
 * depending on authentication plugin
 *
 * Successful Hijack: * -> ACTIVE (same as regular login)
 * Hijack with high sequence number: * -> AWAITING_RESEND (same as regular login)
 * Hijack with low sequence number: disconnect the hijacker and leave main system ACTIVE
 * Hijack with wrong credentials: disconnect the hijacker and leave main system ACTIVE
 *
 * Successful resend: AWAITING_RESEND -> ACTIVE
 *
 * Send test request: ACTIVE -> ACTIVE - but alter the timeout for the next expected heartbeat.
 * Successful Heartbeat: ACTIVE -> ACTIVE - updates the timeout time.
 * Heartbeat Timeout: ACTIVE -> DISCONNECTED
 *
 * Logout request: ACTIVE -> LINGER
 * Logout acknowledgement: LINGER -> DISCONNECTED
 *
 * Manual disable: * -> DISABLED
 */
public enum SessionState
{
    /**
     * Initial state for an outbound session.
     */
    CONNECTING,

    /**
     * A machine has connected to the gateway, but hasn't logged in yet. Initial state of an inbound session.
     */
    CONNECTED,

    /**
     * Session is fully authenticated and ready to execute.
     */
    ACTIVE,

    /**
     * Login had too high a sequence number and a resend or gap fill is required.
     */
    AWAITING_RESEND,

    /**
     * Linger between logout request and a logout acknowledgement. You can do resend processing at this point, but
     * no other messages.
     */
    LINGER,

    /**
     * Session has been disconnected and can't send messages.
     */
    DISCONNECTED,

    /**
     * DISCONNECTED and unable to reconnect.
     */
    DISABLED
}
