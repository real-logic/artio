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

import quickfix.*;
import quickfix.field.MsgType;
import uk.co.real_logic.agrona.LangUtil;

import java.util.ArrayList;
import java.util.List;

public class FakeQuickFixApplication implements Application
{
    private final List<SessionID> logons = new ArrayList<>();
    private final List<SessionID> logouts = new ArrayList<>();
    private final List<Message> messages = new ArrayList<>();

    public void onCreate(final SessionID sessionID)
    {

    }

    public void onLogon(final SessionID sessionID)
    {
        logons.add(sessionID);
    }

    public void onLogout(final SessionID sessionID)
    {
        logouts.add(sessionID);
    }

    public void toAdmin(final Message message, final SessionID sessionID)
    {
        onMessage(message, sessionID);
    }

    public void fromAdmin(final Message message, final SessionID sessionID)
        throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon
    {
        onMessage(message, sessionID);
    }

    public void toApp(final Message message, final SessionID sessionID) throws DoNotSend
    {
        onMessage(message, sessionID);
    }

    public void fromApp(final Message message, final SessionID sessionID)
        throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType
    {
        messages.add(message);
    }

    private void onMessage(final Message message, final SessionID sessionID)
    {
        messages.add(message);
        try
        {
            final String msgType = message.getHeader().getField(new MsgType()).getValue();
            if (MsgType.LOGOUT.equals(msgType))
            {
                logouts.add(sessionID);
            }
        }
        catch (FieldNotFound fieldNotFound)
        {
            LangUtil.rethrowUnchecked(fieldNotFound);
        }
    }

    public List<SessionID> logons()
    {
        return logons;
    }

    public List<SessionID> logouts()
    {
        return logouts;
    }

    public List<Message> messages()
    {
        return messages;
    }
}
