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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

/**
 * LogScanner should be used in a single threaded fashion.
 */
public class ReplayQuery
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    public void query(
        final LogHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
    {

        /*messageFrameHeader.wrap(claimBuffer, claimOffset, messageFrameHeader.size());
        final int actingBlockLength = messageFrameHeader.blockLength();

        claimOffset += messageFrameHeader.size();

        messageFrame.wrap(claimBuffer, claimOffset, actingBlockLength, messageFrameHeader.version());
        final int bodyLength = messageFrame.bodyLength();

        claimOffset += actingBlockLength + SIZE_OF_LENGTH_FIELD;*/
    }
}
