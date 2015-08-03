/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.library.session.Session;

/**
 * Convenient dumb fix message wrapper for testing purposes.
 */
public class FixMessage extends Int2ObjectHashMap<String>
{

    private Session session;

    public FixMessage()
    {
    }

    public String getMessageType()
    {
        return get(Constants.MSG_TYPE);
    }

    public Session session()
    {
        return session;
    }

    public void session(final Session session)
    {
        this.session = session;
    }
}
