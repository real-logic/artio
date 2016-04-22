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
                                final String senderCompId,
                                final String senderSubId,
                                final String senderLocationId,
                                final String targetCompId,
                                final String username,
                                final String password)
    {
        final long correlationId = library.requestSession(sessionId, NO_MESSAGE_REPLAY);
        requests.add(new RequestInfo(
            sessionId, correlationId, senderCompId, senderSubId, senderLocationId, targetCompId, username, password));
    }

    public static final class RequestInfo
    {
        private final long connectionId;
        private final long correlationId;
        private final String senderCompId;
        private final String senderSubId;
        private final String senderLocationId;
        private final String targetCompId;
        private final String username;
        private final String password;

        private RequestInfo(final long connectionId,
                           final long correlationId,
                           final String senderCompId,
                           final String senderSubId,
                           final String senderLocationId,
                           final String targetCompId,
                           final String username,
                           final String password)
        {
            this.connectionId = connectionId;
            this.correlationId = correlationId;
            this.senderCompId = senderCompId;
            this.senderSubId = senderSubId;
            this.senderLocationId = senderLocationId;
            this.targetCompId = targetCompId;
            this.username = username;
            this.password = password;
        }

        public long correlationId()
        {
            return correlationId;
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
