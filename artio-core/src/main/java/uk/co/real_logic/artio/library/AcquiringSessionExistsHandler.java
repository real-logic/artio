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

import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.messages.SessionReplyStatus;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;

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

    public void onSessionExists(
        final FixLibrary library,
        final long surrogateId,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId)
    {
        final Reply<SessionReplyStatus> reply = library.requestSession(
            surrogateId,
            NO_MESSAGE_REPLAY,
            NO_MESSAGE_REPLAY,
            CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS);
        requests.add(new RequestInfo(
            surrogateId, reply, localCompId, localSubId, localLocationId, remoteCompId));
    }

    public static final class RequestInfo
    {
        private final long connectionId;
        private final Reply<SessionReplyStatus> reply;
        private final String senderCompId;
        private final String senderSubId;
        private final String senderLocationId;
        private final String targetCompId;

        private RequestInfo(
            final long connectionId,
            final Reply<SessionReplyStatus> reply,
            final String senderCompId,
            final String senderSubId,
            final String senderLocationId,
            final String targetCompId)
        {
            this.connectionId = connectionId;
            this.reply = reply;
            this.senderCompId = senderCompId;
            this.senderSubId = senderSubId;
            this.senderLocationId = senderLocationId;
            this.targetCompId = targetCompId;
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
    }
}
