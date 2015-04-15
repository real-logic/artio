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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.fix_gateway.framer.Framer;
import uk.co.real_logic.fix_gateway.framer.FramerCommand;
import uk.co.real_logic.fix_gateway.sender.Sender;
import uk.co.real_logic.fix_gateway.sender.SenderCommand;

public class NewSessionId implements FramerCommand, SenderCommand
{
    private final Object compositeId;
    private final long surrogateId;

    public NewSessionId(final Object compositeId, final long surrogateId)
    {
        this.compositeId = compositeId;
        this.surrogateId = surrogateId;
    }

    public void execute(final Framer framer)
    {
        framer.onNewSessionId(compositeId, surrogateId);
    }

    public void execute(final Sender sender)
    {
        sender.onNewSessionId(compositeId, surrogateId);
    }
}
