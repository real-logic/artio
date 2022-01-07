/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import b3.entrypoint.fixp.sbe.*;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.hamcrest.Matchers;
import org.junit.Test;
import uk.co.real_logic.artio.engine.EngineConfiguration;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.getMessagesFromArchive;

public class BinaryEntryPointArchiveScannerIntegrationTest extends AbstractBinaryEntryPointSystemTest
{
    private boolean follow = false;

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhilstGatewayRunningOneStream() throws IOException
    {
        setupAndExchangeMessages();

        assertOutboundArchiveContainsMessages();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhilstGatewayRunningBothStreams() throws IOException
    {
        setupAndExchangeMessages();

        assertArchiveContainsBothMessages();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhilstGatewayRunningBothStreamsFollowMode() throws IOException
    {
        follow = true;

        setupAndExchangeMessages();

        assertArchiveContainsBothMessages();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhenGatewayStoppedOneStream() throws IOException
    {
        setupAndExchangeMessages();

        closeArtio();

        assertOutboundArchiveContainsMessages();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canScanArchiveWhenGatewayStoppedBothStreams() throws IOException
    {
        setupAndExchangeMessages();

        closeArtio();

        assertArchiveContainsBothMessages();
    }

    private void assertOutboundArchiveContainsMessages()
    {
        final EngineConfiguration configuration = engine.configuration();
        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(configuration.outboundLibraryStream());

        assertArchiveContains(configuration, queryStreamIds);
    }

    private void assertArchiveContainsBothMessages()
    {
        final EngineConfiguration configuration = engine.configuration();
        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(configuration.outboundLibraryStream());
        queryStreamIds.add(configuration.inboundLibraryStream());

        assertArchiveContains(configuration, queryStreamIds);
    }

    private void assertArchiveContains(final EngineConfiguration configuration, final IntHashSet queryStreamIds)
    {
        final IntArrayList templateIds = new IntArrayList();
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        try
        {
            getMessagesFromArchive(configuration, queryStreamIds, null,
                (fixPMessage, buffer, offset, ignore) ->
                {
                    header.wrap(buffer, offset + SOFH_LENGTH);
                    templateIds.add(header.templateId());

                    if (follow && templateIds.size() >= 7)
                    {
                        throw new TestTerminationException();
                    }
                }, follow);
        }
        catch (final TestTerminationException e)
        {
            // Deliberately blank. This is just used to force termination in follow mode.
        }

        assertThat(templateIds, Matchers.hasItems(
            NegotiateResponseDecoder.TEMPLATE_ID,
            EstablishAckDecoder.TEMPLATE_ID,
            ExecutionReport_NewDecoder.TEMPLATE_ID,
            TerminateDecoder.TEMPLATE_ID));
    }

    private void setupAndExchangeMessages() throws IOException
    {
        setupArtio();

        connectAndExchangeBusinessMessage();
    }

    static class TestTerminationException extends RuntimeException
    {
    }
}
