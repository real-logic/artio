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
package uk.co.real_logic.fix_gateway.session_management;

/**
 * <h1>Transitions</h1>
 *
 * Successful Login: CONNECTION_ESTABLISHED -> AUTHENTICATED
 * Login with high sequence number: CONNECTION_ESTABLISHED -> AWAITING_RESEND
 * Login with low sequence number: CONNECTION_ESTABLISHED -> DISCONNECTED
 *
 * Successful Hijack: * -> AUTHENTICATED
 * Hijack with high sequence number: TODO: what's the right behaviour?
 * Hijack with low sequence number: TODO: what's the right behaviour - disconnect hijack or disconnect everything?
 *
 * Successful resend: AWAITING_RESEND -> AUTHENTICATED
 *
 * Send test request: AUTHENTICATED -> AUTHENTICATED - but alter the timeout for the next expected heartbeat.
 * Successful Heartbeat: AUTHENTICATED -> AUTHENTICATED.
 * Heartbeat Timeout: AUTHENTICATED -> DISCONNECTED
 *
 * Logout request: AUTHENTICATED -> LINGER
 * Logout acknowledgement: LINGER -> DISCONNECTED
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
    CONNECTION_ESTABLISHED,

    /**
     * Session is fully authenticated and ready to execute.
     */
    AUTHENTICATED,

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
}
