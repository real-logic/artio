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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;

/**
 * This strategy creates the composite key that uniquely identifies Session Ids. This is a strategy
 * because different gateways identify themselves based upon different sets of fields, Common fields
 * include:
 * <ul>
 *     <li>Sender Company Id + Target Company Id</li>
 *     <li>Sender Company Id + Sender Sub Id + Target Company Id</li>
 *     <li>Sender Company Id + Sender Sub Id + Sender Location Id + Target Company Id</li>
 * </ul>
 *
 * <p>
 * Implementations should define their own composite session key type, which must provide correct value based
 * equals/hashcode implementation. Strategy should be stateless.
 *
 * @see SenderAndTargetSessionIdStrategy
 * @see SenderTargetAndSubSessionIdStrategy
 */
public interface SessionIdStrategy
{
    /**
     * Creates the composite session key when you accept a logon.
     *
     * @param header the header of the logon message.
     * @return the composite session key.
     */
    Object onAcceptorLogon(final HeaderDecoder header);

    /**
     * Creates the composite session key when you initiate a logon.
     *
     * @param senderCompId the sender company id, always present.
     * @param senderSubId the sender sub id, nullable.
     * @param senderLocationId the sender location id, nullable.
     * @param targetCompId the target company id, always present.
     * @return the composite session key.
     */
    Object onInitiatorLogon(String senderCompId, String senderSubId, String senderLocationId, String targetCompId);

    /**
     * Sets up an outbound message header with the composite session key.
     *
     * @param compositeKey the composite session key.
     * @param headerEncoder the outbound message header.
     */
    void setupSession(final Object compositeKey, final HeaderEncoder headerEncoder);
}
