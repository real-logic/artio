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

/**
 * .
 */
class RequestSessionReply extends LibraryReply<SessionReplyStatus>
{
    private final long sessionId;
    private final int lastReceivedSequenceNumber;
    private final int sequenceIndex;

    private boolean requiresResend;

    RequestSessionReply(
        final LibraryPoller libraryPoller,
        final long latestReplyArrivalTime,
        final long sessionId,
        final int lastReceivedSequenceNumber,
        final int sequenceIndex)
    {
        super(libraryPoller, latestReplyArrivalTime);
        this.sessionId = sessionId;
        this.lastReceivedSequenceNumber = lastReceivedSequenceNumber;
        this.sequenceIndex = sequenceIndex;
        sendMessage();
    }

    private void sendMessage()
    {
        final long position = libraryPoller.saveRequestSession(sessionId, correlationId, lastReceivedSequenceNumber);

        requiresResend = position < 0;
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
