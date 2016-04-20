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
 * {@link NewConnectHandler} implementation that tries to acquire any session
 * that has connected to the engine. Very useful for implementing simple 1-to-1
 * topology configurations between engine and library.
 */
public class AcquiringNewConnectHandler implements NewConnectHandler
{
    private final List<RequestInfo> requests = new ArrayList<>();

    public void onConnect(final FixLibrary library, final long connectionId, final String address)
    {
        final long correlationId = library.requestSession(connectionId, NO_MESSAGE_REPLAY);
        requests.add(new RequestInfo(connectionId, address, correlationId));
    }

    public List<RequestInfo> requests()
    {
        return requests;
    }

    public static class RequestInfo
    {
        private final long connectionId;
        private final String address;
        private final long correlationId;

        public RequestInfo(final long connectionId,
                           final String address,
                           final long correlationId)
        {
            this.connectionId = connectionId;
            this.address = address;
            this.correlationId = correlationId;
        }

        public long correlationId()
        {
            return correlationId;
        }

        public String address()
        {
            return address;
        }

        public long connectionId()
        {
            return connectionId;
        }

    }
}
