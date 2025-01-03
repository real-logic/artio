/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.session;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;

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
 * In all cases sender and target are defined from your own perspective of the connection.
 */
public interface SessionIdStrategy
{
    char[] MISSING_COMP_ID = "missing".toCharArray();

    int INSUFFICIENT_SPACE = -1;

    static char[] checkMissing(final char[] providedCompId)
    {
        return providedCompId.length == 0 ? MISSING_COMP_ID : providedCompId;
    }

    static SessionIdStrategy senderAndTarget()
    {
        return new SenderAndTargetSessionIdStrategy();
    }

    static SessionIdStrategy senderTargetAndSub()
    {
        return new SenderTargetAndSubSessionIdStrategy();
    }

    /**
     * Creates the composite session key when you accept a logon.
     *
     * @param header the header of the logon message.
     * @return the composite session key.
     * @throws IllegalArgumentException if the header is missing a required field then an IllegalArgumentException
     * can be thrown.
     */
    CompositeKey onAcceptLogon(SessionHeaderDecoder header) throws IllegalArgumentException;

    /**
     * Creates the composite session key when you initiate a logon.
     *
     * @param localCompId the sender company id, always present.
     * @param localSubId the sender sub id, nullable.
     * @param localLocationId the sender location id, nullable.
     * @param remoteCompId the target company id, always present.
     * @param remoteSubId the target sub id, nullable.
     * @param remoteLocationId the target location id, nullable.
     * @return the composite session key.
     */
    CompositeKey onInitiateLogon(
        String localCompId,
        String localSubId,
        String localLocationId,
        String remoteCompId,
        String remoteSubId,
        String remoteLocationId);

    /**
     * Sets up an outbound message header with the composite session key.
     *
     * @param compositeKey the composite session key.
     * @param headerEncoder the outbound message header.
     */
    void setupSession(CompositeKey compositeKey, SessionHeaderEncoder headerEncoder);

    /**
     * Saves the given composite key to a buffer.
     *
     * @param compositeKey the key to save
     * @param buffer the buffer to save it to
     * @param offset the offset within the buffer to start saving at
     * @return the length used to save the key, or {@link SessionIdStrategy#INSUFFICIENT_SPACE} otherwise
     */
    int save(CompositeKey compositeKey, MutableDirectBuffer buffer, int offset);

    /**
     * Loads a composite key from a buffer.
     *
     * @param buffer the buffer to save it to
     * @param offset the offset within the buffer to start saving at
     * @param length the length within the buffer to read from
     * @return the loaded key or null if there was a failure.
     */
    CompositeKey load(DirectBuffer buffer, int offset, int length);

    /**
     * Check that the header of a message matches the expected composite key of the session.
     *
     * @param compositeKey the expected key.
     * @param header the header of the message that is to be validated.
     * @return the tag number that is invalid or 0 if everything is valid.
     */
    int validateCompIds(CompositeKey compositeKey, SessionHeaderDecoder header);
}
