/*
 * Copyright 2022 Monotonic Ltd.
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
/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.Image;
import io.aeron.Subscription;
import uk.co.real_logic.artio.storage.messages.Bool;
import uk.co.real_logic.artio.storage.messages.ConnectionBackpressureDecoder;
import uk.co.real_logic.artio.storage.messages.MessageHeaderDecoder;

final class ReproductionLogReader
{
    static ReproductionLog read(final Subscription recordingSubscription)
    {
        if (recordingSubscription == null)
        {
            return null;
        }

        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
        final ConnectionBackpressureDecoder connectionBackpressure = new ConnectionBackpressureDecoder();

        final ReproductionLog reproductionLog = new ReproductionLog();

        Image recordingImage = null;
        while (recordingSubscription.hasNoImages())
        {
            Thread.yield();
        }
        recordingImage = recordingSubscription.imageAtIndex(0);

        while (!recordingImage.isEndOfStream() && !recordingImage.isClosed())
        {
            recordingSubscription.poll((buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder messageHdr = messageHeader;

                messageHdr.wrap(buffer, offset);
                final int templateId = messageHdr.templateId();
                if (templateId == ConnectionBackpressureDecoder.TEMPLATE_ID)
                {
                    final ConnectionBackpressureDecoder connectionBackpress = connectionBackpressure;
                    connectionBackpress.wrap(buffer, offset, messageHdr.blockLength(), messageHdr.version());

                    reproductionLog.put(
                        connectionBackpress.connectionId(),
                        new ConnectionBackPressureEvent(
                        connectionBackpress.sequenceNumber(),
                        connectionBackpress.isReplay() == Bool.TRUE,
                        connectionBackpress.written()));
                }
            }, 10);

            Thread.yield();
        }

        return reproductionLog;
    }
}
