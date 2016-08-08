/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.library;

import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.session.Session;

/**
 * .
 */
class ReleaseToGatewayReply extends Reply<SessionReplyStatus>
{
    private final Session session;

    private boolean requiresResend;

    ReleaseToGatewayReply(final LibraryPoller libraryPoller, final long latestReplyArrivalTime, final Session session)
    {
        super(libraryPoller, latestReplyArrivalTime);
        this.session = session;
        sendMessage();
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
            libraryPoller.removeSession(session);
            session.disable();
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
