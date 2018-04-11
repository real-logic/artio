/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.library;

import uk.co.real_logic.artio.messages.GatewayError;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;

/**
 * .
 */
class ReleaseToGatewayReply extends LibraryReply<SessionReplyStatus>
{
    private final Session session;

    private boolean requiresResend;

    ReleaseToGatewayReply(final LibraryPoller libraryPoller, final long latestReplyArrivalTime, final Session session)
    {
        super(libraryPoller, latestReplyArrivalTime);
        this.session = session;
        if (libraryPoller.isConnected())
        {
            sendMessage();
        }
    }

    private void sendMessage()
    {
        final long position = libraryPoller.saveReleaseSession(session, correlationId);

        requiresResend = position < 0;
    }

    void onComplete(final SessionReplyStatus result)
    {
        if (result == SessionReplyStatus.OK)
        {
            libraryPoller.disableSession(session);
        }

        super.onComplete(result);
    }

    void onError(final GatewayError errorType, final String errorMessage)
    {
    }

    boolean poll(final long timeInMs)
    {
        if (requiresResend)
        {
            sendMessage();
        }

        return super.poll(timeInMs);
    }
}
