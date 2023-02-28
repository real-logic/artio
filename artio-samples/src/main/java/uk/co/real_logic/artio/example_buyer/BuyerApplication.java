/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.example_buyer;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import uk.co.real_logic.artio.SampleUtil;

import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME;
import static io.aeron.driver.ThreadingMode.SHARED;

public final class BuyerApplication
{
    public static final String AERON_DIRECTORY_NAME = "buyer";
    public static final String AERON_ARCHIVE_DIRECTORY_NAME = "buyer-archive";
    // Channels are specified in order to avoid a collision with the sample ExchangeApplication when running on the same
    // box
    public static final String RECORDING_EVENTS_CHANNEL = "aeron:udp?endpoint=localhost:9030";

    public static void main(final String[] args) throws InterruptedException
    {
        System.setProperty(CONTROL_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:9010");
        System.setProperty(CONTROL_RESPONSE_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:9020");

        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirDeleteOnStart(true)
            .aeronDirectoryName(AERON_DIRECTORY_NAME);

        final Archive.Context archiveContext = new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(true)
            .aeronDirectoryName(AERON_DIRECTORY_NAME)
            .recordingEventsChannel(RECORDING_EVENTS_CHANNEL)
            .archiveDirectoryName(AERON_ARCHIVE_DIRECTORY_NAME);

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(context, archiveContext))
        {
            SampleUtil.runAgentUntilSignal(
                new BuyerAgent(),
                driver.mediaDriver());
        }
    }
}
