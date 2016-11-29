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

import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;

/**
 * {@link SessionExistsHandler} implementation that tries to acquire any session
 * that has been accepted by the engine. Very useful for implementing simple 1-to-1
 * topology configurations between engine and library.
 */
public class AcquiringSessionExistsHandler implements SessionExistsHandler
{
    private final List<RequestInfo> requests = new ArrayList<>();

    public List<RequestInfo> requests()
    {
        return requests;
    }

    public void onSessionExists(final FixLibrary library,
                                final long sessionId,
                                final String acceptorCompId,
                                final String acceptorSubId,
                                final String acceptorLocationId,
                                final String initiatorCompId,
                                final String username,
                                final String password)
    {
        final Reply<SessionReplyStatus> reply = library.requestSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
        requests.add(new RequestInfo(
            sessionId, reply, acceptorCompId, acceptorSubId, acceptorLocationId, initiatorCompId, username, password));
    }

    public static final class RequestInfo
    {
        private final long connectionId;
        private final Reply<SessionReplyStatus> reply;
        private final String senderCompId;
        private final String senderSubId;
        private final String senderLocationId;
        private final String targetCompId;
        private final String username;
        private final String password;

        private RequestInfo(final long connectionId,
                           final Reply<SessionReplyStatus> reply,
                           final String senderCompId,
                           final String senderSubId,
                           final String senderLocationId,
                           final String targetCompId,
                           final String username,
                           final String password)
        {
            this.connectionId = connectionId;
            this.reply = reply;
            this.senderCompId = senderCompId;
            this.senderSubId = senderSubId;
            this.senderLocationId = senderLocationId;
            this.targetCompId = targetCompId;
            this.username = username;
            this.password = password;
        }

        public Reply<SessionReplyStatus> reply()
        {
            return reply;
        }

        public long connectionId()
        {
            return connectionId;
        }

        public String senderCompId()
        {
            return senderCompId;
        }

        public String senderSubId()
        {
            return senderSubId;
        }

        public String senderLocationId()
        {
            return senderLocationId;
        }

        public String targetCompId()
        {
            return targetCompId;
        }

        public String username()
        {
            return username;
        }

        public String password()
        {
            return password;
        }
    }
}
