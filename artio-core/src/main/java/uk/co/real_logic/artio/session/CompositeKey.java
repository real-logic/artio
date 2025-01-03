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

/**
 * The full identifying key of the session in question. Methods may return
 * empty Strings if the {@link SessionIdStrategy} doesn't use that information
 * as part of session identification.
 *
 * Implementors should provide appropriate equals/hashcode methods.
 */
public interface CompositeKey
{
    /**
     * Gets the Local Comp Id. This is the senderCompId of messages sent by this session.
     *
     * @return the Local Comp Id.
     */
    String localCompId();

    /**
     * Gets the Local Sub Id. This is the senderSubId of messages sent by this session.
     *
     * @return the Local Sub Id.
     */
    String localSubId();

    /**
     * Gets the Local Location Id. This is the senderLocationId of messages sent by this session.
     *
     * @return the Local Location Id.
     */
    String localLocationId();

    /**
     * Gets the Remote Comp Id. This is the targetCompId of messages sent by this session.
     *
     * @return the Remote Comp Id.
     */
    String remoteCompId();

    /**
     * Gets the Remote Sub Id. This is the targetSubId of messages sent by this session.
     *
     * @return the Remote Sub Id.
     */
    String remoteSubId();

    /**
     * Gets the Remote Location Id. This is the targetLocationId of messages sent by this session.
     *
     * @return the Remote Location Id.
     */
    String remoteLocationId();
}
