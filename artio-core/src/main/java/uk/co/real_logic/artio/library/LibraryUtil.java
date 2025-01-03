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
package uk.co.real_logic.artio.library;

import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.LangUtil;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.session.Session;


/**
 * Utility methods for blocking operations on the {@link FixLibrary}.
 *
 * Kept in a separate class in order to ensure that the main {@link FixLibrary} API is kept non-blocking.
 */
public final class LibraryUtil
{

    private static final int DEFAULT_FRAGMENT_LIMIT = 10;

    /**
     * Initiate a FIX session with a FIX acceptor. This method returns a reply object
     * wrapping the Session itself.
     *
     * @param library the library to attempt to initiate from.
     * @param configuration the configuration to use for the session.
     * @param attempts the number of times to attempt to connect to a gateway.
     * @param idleStrategy the {@link IdleStrategy} to use when polling.
     * @return the session object for the session that you've initiated.
     * @throws IllegalStateException if you're trying to initiate two sessions at the same time or if there's a timeout talking to
     *         the {@link uk.co.real_logic.artio.engine.FixEngine}.
     *         This probably indicates that there's a problem in your code or that your engine isn't running.
     * @throws FixGatewayException
     *         if you're unable to connect to the accepting gateway.
     *         This probably indicates a configuration problem related to the external gateway.
     * @throws TimeoutException the connection has timed out <code>attempts</code> number of times.
     * @throws IllegalArgumentException if attempts is &lt; 1
     */
    public static Session initiate(
        final FixLibrary library,
        final SessionConfiguration configuration,
        final int attempts,
        final IdleStrategy idleStrategy) throws IllegalStateException, FixGatewayException, TimeoutException
    {
        return initiate(library, configuration, attempts, idleStrategy, DEFAULT_FRAGMENT_LIMIT);
    }

    /**
     * Initiate a FIX session with a FIX acceptor. This method returns a reply object
     * wrapping the Session itself.
     *
     * @param library the library to attempt to initiate from.
     * @param configuration the configuration to use for the session.
     * @param attempts the number of times to attempt to connect to a gateway.
     * @param idleStrategy the {@link IdleStrategy} to use when polling.
     * @param fragmentLimit the number of messages to poll on the library before idling.
     * @return the session object for the session that you've initiated.
     * @throws IllegalStateException if you're trying to initiate two sessions at the same time or if there's a timeout talking to
     *         the {@link uk.co.real_logic.artio.engine.FixEngine}.
     *         This probably indicates that there's a problem in your code or that your engine isn't running.
     * @throws FixGatewayException
     *         if you're unable to connect to the accepting gateway.
     *         This probably indicates a configuration problem related to the external gateway.
     * @throws TimeoutException the connection has timed out <code>attempts</code> number of times.
     * @throws IllegalArgumentException if attempts is &lt; 1
     */
    private static Session initiate(
        final FixLibrary library,
        final SessionConfiguration configuration,
        final int attempts,
        final IdleStrategy idleStrategy,
        final int fragmentLimit)
    {
        if (attempts < 1)
        {
            throw new IllegalArgumentException("Attempts should be >= 1, but is " + attempts);
        }

        Reply<Session> reply = null;
        for (int i = 0; i < attempts; i++)
        {
            reply = library.initiate(configuration);
            while (reply.isExecuting())
            {
                idleStrategy.idle(library.poll(fragmentLimit));
            }

            if (reply.hasCompleted())
            {
                return reply.resultIfPresent();
            }
        }

        if (reply.hasTimedOut())
        {
            throw new TimeoutException("reply timeout", AeronException.Category.ERROR);
        }
        else
        {
            LangUtil.rethrowUnchecked(reply.error());
            // never executed:
            return null;
        }
    }
}
