/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.admin;

import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.messages.AllFixSessionsReplyDecoder;
import uk.co.real_logic.artio.messages.GatewayError;
import uk.co.real_logic.artio.messages.SlowStatus;

import java.util.ArrayList;
import java.util.List;

class AdminEndPointHandler
{
    private long expectedCorrelationId;
    private boolean hasReceivedReply;

    private List<FixAdminSession> allFixSessions;

    private GatewayError errorType;
    private String errorMessage;

    public boolean hasReceivedReply()
    {
        return hasReceivedReply;
    }

    public List<FixAdminSession> allFixSessions()
    {
        return allFixSessions;
    }

    public void onAllFixSessionsReply(
        final long correlationId,
        final AllFixSessionsReplyDecoder.SessionsDecoder sessions)
    {
        if (correlationId == expectedCorrelationId)
        {
            allFixSessions = new ArrayList<>();

            while (sessions.hasNext())
            {
                sessions.next();
                allFixSessions.add(new FixAdminSession(
                    sessions.sessionId(),
                    sessions.connectionId(),
                    sessions.lastReceivedSequenceNumber(),
                    sessions.lastSentSequenceNumber(),
                    sessions.lastLogonTime(),
                    sessions.sequenceIndex(),
                    sessions.slowStatus() == SlowStatus.SLOW,
                    sessions.address(),
                    new AdminCompositeKey(
                    sessions.localCompId(),
                    sessions.localSubId(),
                    sessions.localLocationId(),
                    sessions.remoteCompId(),
                    sessions.remoteSubId(),
                    sessions.remoteLocationId())
                ));
            }

            hasReceivedReply = true;
        }
    }

    public void expectedCorrelationId(final long correlationId)
    {
        expectedCorrelationId = correlationId;
        hasReceivedReply = false;
    }

    public <T> T checkError()
    {
        if (errorType != null)
        {
            try
            {
                throw new FixGatewayException(errorMessage);
            }
            finally
            {
                errorType = null;
                errorMessage = null;
            }
        }
        return (T)null;
    }

    public void onGenericAdminReply(final long correlationId, final GatewayError errorType, final String message)
    {
        if (correlationId == expectedCorrelationId)
        {
            if (errorType != null && errorType != GatewayError.NULL_VAL)
            {
                this.errorType = errorType;
                this.errorMessage = message;
            }
            hasReceivedReply = true;
        }
    }
}
