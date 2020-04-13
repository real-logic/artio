/*
 * Copyright 2020 Monotonic Ltd.
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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.MetaDataStatus;

import java.util.concurrent.locks.LockSupport;

import static org.agrona.BitUtil.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MetaDataTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int UPDATE_OFFSET = 1;
    private static final short UPDATE_VALUE = 128;

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID)
            .deleteLogFileDirOnStart(true);
        acceptingConfig.printErrorMessages(false);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = 10_000L)
    public void shouldReadWrittenSessionMetaData()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);
        writeBuffer.putInt(0, META_DATA_VALUE);

        writeMetaData(writeBuffer);

        final UnsafeBuffer readBuffer = readSuccessfulMetaData(writeBuffer);
        assertEquals(META_DATA_VALUE, readBuffer.getInt(0));
    }

    @Test(timeout = 10_000L)
    public void shouldReadWrittenSessionSendMetaData()
    {
        acquireAcceptingSession();

        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);
        writeBuffer.putInt(0, META_DATA_VALUE);

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID(testReqId());
        send(writeBuffer, testRequest, 0);

        assertEventuallyTrue("Failed to read meta data", () ->
        {
            final UnsafeBuffer readBuffer = readSuccessfulMetaData(writeBuffer);
            assertEquals(META_DATA_VALUE, readBuffer.getInt(0));

            LockSupport.parkNanos(10_000L);
        });

        final UnsafeBuffer updateBuffer = updateBuffer();

        send(updateBuffer, testRequest, UPDATE_OFFSET);

        updateExpectedBuffer(writeBuffer, updateBuffer);

        assertEventuallyTrue("Failed to read meta data", () ->
        {
            final UnsafeBuffer readBuffer = readSuccessfulMetaData(writeBuffer);

            assertEquals(writeBuffer, readBuffer);

            LockSupport.parkNanos(10_000L);
        });
    }

    @Test(timeout = 10_000L)
    public void shouldReceiveSessionMetaDataWhenSessionAcquired()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);
        writeBuffer.putInt(0, META_DATA_VALUE);
        writeMetaData(writeBuffer);

        acquireAcceptingSession();
        assertEquals(MetaDataStatus.OK, acceptingHandler.lastSessionMetaDataStatus());
        final DirectBuffer readBuffer = acceptingHandler.lastSessionMetaData();
        assertEquals(META_DATA_VALUE, readBuffer.getInt(0));
        assertEquals(SIZE_OF_INT, readBuffer.capacity());
    }

    @Test(timeout = 10_000L)
    public void shouldNotReceiveSessionMetaDataWhenSessionAcquiredWithNoMetaData()
    {
        acquireAcceptingSession();
        assertEquals(MetaDataStatus.NO_META_DATA, acceptingHandler.lastSessionMetaDataStatus());
        final DirectBuffer readBuffer = acceptingHandler.lastSessionMetaData();
        assertEquals(0, readBuffer.capacity());
    }

    @Test(timeout = 10_000L)
    public void shouldUpdateWrittenSessionMetaDataFittingWithinSlot()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);

        writeBuffer.putInt(0, META_DATA_WRONG_VALUE);
        writeMetaData(writeBuffer);

        writeBuffer.putInt(0, META_DATA_VALUE);
        writeMetaData(writeBuffer);

        UnsafeBuffer readBuffer = readSuccessfulMetaData(writeBuffer);
        assertEquals(META_DATA_VALUE, readBuffer.getInt(0));

        final UnsafeBuffer updateBuffer = updateBuffer();

        final Reply<MetaDataStatus> reply = writeMetaData(updateBuffer, META_DATA_SESSION_ID, UPDATE_OFFSET);
        assertEquals(MetaDataStatus.OK, reply.resultIfPresent());

        updateExpectedBuffer(writeBuffer, updateBuffer);

        readBuffer = readSuccessfulMetaData(writeBuffer);
        assertEquals(writeBuffer, readBuffer);
    }

    @Test(timeout = 10_000L)
    public void shouldUpdateWrittenSessionMetaDataTooBigForOldSlot()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);

        writeBuffer.putInt(0, META_DATA_WRONG_VALUE);
        writeMetaData(writeBuffer);

        final UnsafeBuffer bigWriteBuffer = new UnsafeBuffer(new byte[SIZE_OF_LONG]);
        bigWriteBuffer.putLong(0, META_DATA_VALUE);
        writeMetaData(bigWriteBuffer);

        UnsafeBuffer readBuffer = readSuccessfulMetaData(bigWriteBuffer);
        assertEquals(META_DATA_VALUE, readBuffer.getInt(0));

        final UnsafeBuffer updateBuffer = updateBuffer();

        final int bigUpdateOffset = SIZE_OF_LONG;
        final Reply<MetaDataStatus> reply = writeMetaData(updateBuffer, META_DATA_SESSION_ID, bigUpdateOffset);
        assertEquals(MetaDataStatus.OK, reply.resultIfPresent());

        final UnsafeBuffer aggregatedBuffer = new UnsafeBuffer(new byte[bigUpdateOffset + SIZE_OF_SHORT]);
        aggregatedBuffer.putLong(0, META_DATA_VALUE);
        aggregatedBuffer.putShort(bigUpdateOffset, UPDATE_VALUE);

        readBuffer = readSuccessfulMetaData(aggregatedBuffer);
        assertEquals(aggregatedBuffer, readBuffer);
    }

    @Test(timeout = 10_000L)
    public void shouldReceiveReadErrorForUnwrittenSessionMetaData()
    {
        assertNoMetaData();
    }

    @Test(timeout = 10_000L)
    public void shouldReceiveReadErrorForMetaDataWithUnknownSession()
    {
        assertUnknownSessionMetaData(META_DATA_WRONG_SESSION_ID);
    }

    @Test(timeout = 10_000L)
    public void shouldReceiveWriteErrorForMetaDataWithUnknownSession()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);
        final Reply<?> reply = writeMetaData(writeBuffer, META_DATA_WRONG_SESSION_ID);
        assertEquals(MetaDataStatus.UNKNOWN_SESSION, reply.resultIfPresent());
    }

    @Test(timeout = 10_000L)
    public void shouldResetMetaDataWhenSequenceNumberResetsWithLogon()
    {
        writeMetaDataThenDisconnect();

        // Support reading meta data after logout, but before sequence number reset
        final FakeMetadataHandler handler = readMetaData(META_DATA_SESSION_ID);
        assertEquals(MetaDataStatus.OK, handler.status());

        connectSessions();

        assertNoMetaData();
    }

    @Test(timeout = 10_000L)
    public void shouldResetMetaDataWhenSequenceNumberResetsWhenSessionIdExplicitlyReset()
    {
        writeMetaDataThenDisconnect();

        final Reply<?> reply = acceptingEngine.resetSequenceNumber(META_DATA_SESSION_ID);
        testSystem.awaitCompletedReplies(reply);

        assertNoMetaData();
    }

    @Test(timeout = 10_000L)
    public void shouldResetMetaDataWhenSequenceNumberResetsWithExplicitResetSessionIds()
    {
        writeMetaDataThenDisconnect();

        final Reply<?> reply = acceptingEngine.resetSessionIds(null);
        testSystem.awaitCompletedReplies(reply);

        assertUnknownSessionMetaData(META_DATA_SESSION_ID);

        connectSessions();
        acquireAcceptingSession();

        assertNoMetaData();
    }

    private void assertUnknownSessionMetaData(final long sessionId)
    {
        final FakeMetadataHandler handler = readMetaData(sessionId);
        assertEquals(MetaDataStatus.UNKNOWN_SESSION, handler.status());
    }

    private void assertNoMetaData()
    {
        final FakeMetadataHandler handler = readMetaData(META_DATA_SESSION_ID);
        assertEquals(MetaDataStatus.NO_META_DATA, handler.status());
    }

    private void writeMetaDataThenDisconnect()
    {
        writeMetaData();

        acquireAcceptingSession();
        disconnectSessions();
    }

    private void send(
        final UnsafeBuffer writeBuffer, final TestRequestEncoder testRequest, final int metaDataUpdateOffset)
    {
        assertThat(acceptingSession.trySend(testRequest, writeBuffer, metaDataUpdateOffset), greaterThan(0L));
    }

    private UnsafeBuffer updateBuffer()
    {
        final UnsafeBuffer updateBuffer = new UnsafeBuffer(new byte[SIZE_OF_SHORT]);
        updateBuffer.putShort(0, UPDATE_VALUE);
        return updateBuffer;
    }

    private void updateExpectedBuffer(final UnsafeBuffer writeBuffer, final UnsafeBuffer updateBuffer)
    {
        writeBuffer.putBytes(UPDATE_OFFSET, updateBuffer, 0, SIZE_OF_SHORT);
    }
}
