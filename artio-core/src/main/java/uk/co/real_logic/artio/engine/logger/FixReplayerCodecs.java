/*
 * Copyright 2019 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;

class FixReplayerCodecs
{
    private final FixDictionary dictionary;
    private final AbstractResendRequestDecoder resendRequest;
    private final UtcTimestampEncoder timestampEncoder;
    private final EpochNanoClock nanoClock;
    private GapFillEncoder gapFillEncoder;

    FixReplayerCodecs(
        final Class<? extends FixDictionary> fixDictionaryType,
        final UtcTimestampEncoder timestampEncoder,
        final EpochNanoClock nanoClock)
    {
        dictionary = FixDictionary.of(fixDictionaryType);
        this.timestampEncoder = timestampEncoder;
        this.nanoClock = nanoClock;
        resendRequest = dictionary.makeResendRequestDecoder();
    }

    AbstractResendRequestDecoder resendRequest()
    {
        return resendRequest;
    }

    GapFillEncoder gapFillEncoder()
    {
        if (gapFillEncoder == null)
        {
            gapFillEncoder = makeGapFillEncoder();
        }

        return gapFillEncoder;
    }

    GapFillEncoder makeGapFillEncoder()
    {
        return new GapFillEncoder(dictionary.makeSequenceResetEncoder(), timestampEncoder, nanoClock);
    }

    FixDictionary dictionary()
    {
        return dictionary;
    }
}
