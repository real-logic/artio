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
package uk.co.real_logic.generic_callback_api;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageHandler;

// TODO: figure out what we should use as our example to implement in all 3 APIs
public class MessageProcessor implements FixMessageHandler
{

    @Override
    public void onStartMessage()
    {

    }

    @Override
    public void onField(int tag, DirectBuffer buffer, int offset, int length)
    {

    }

    @Override
    public void onGroupBegin(int tag, int numberOfElements)
    {

    }

    @Override
    public void onGroupEnd(int tag)
    {

    }

    @Override
    public void onEndMessage()
    {

    }
}
