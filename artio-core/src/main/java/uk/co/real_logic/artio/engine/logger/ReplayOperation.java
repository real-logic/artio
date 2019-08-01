/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;

/**
 * A continuable replay operation that can retried.
 *
 * Each object is single threaded, but different objects used on different threads.
 */
public class ReplayOperation
{
    private final MessageTracker messageTracker;
    private final ControlledFragmentAssembler assembler;

    private final List<RecordingRange> ranges;
    private final AeronArchive aeronArchive;
    private final ErrorHandler errorHandler;
    private final int archiveReplayStream;
    private final LogTag logTag;
    private final CountersReader countersReader;
    private final Subscription subscription;

    // fields reset for each recordingRange
    private int replayedMessages = 0;
    private RecordingRange recordingRange;
    private int aeronSessionId;
    private Image image;

    ReplayOperation(
        final ControlledFragmentHandler handler,
        final List<RecordingRange> ranges,
        final AeronArchive aeronArchive,
        final ErrorHandler errorHandler,
        final Subscription subscription,
        final int archiveReplayStream,
        final LogTag logTag)
    {
        messageTracker = new MessageTracker(logTag, handler);
        assembler = new ControlledFragmentAssembler(messageTracker);

        this.ranges = ranges;
        this.aeronArchive = aeronArchive;
        this.errorHandler = errorHandler;
        this.archiveReplayStream = archiveReplayStream;
        this.logTag = logTag;

        final Aeron aeron = aeronArchive.context().aeron();
        countersReader = aeron.countersReader();
        this.subscription = subscription;
    }

    /**
     * Attempt a replay step
     *
     * @return true if complete
     */
    public boolean attemptReplay()
    {
        return attemptReplayStep();
    }

    private boolean attemptReplayStep()
    {
        if (recordingRange == null)
        {
            DebugLogger.log(logTag, "Acquiring Recording Range");

            if (ranges.isEmpty())
            {
                return true;
            }

            recordingRange = ranges.get(0);
            DebugLogger.log(logTag,
                "ReplayOperation : Attempting Recording Range: %s%n",
                recordingRange);

            messageTracker.sessionId = recordingRange.sessionId;

            final long beginPosition = recordingRange.position;
            final long length = recordingRange.length;
            final long endPosition = beginPosition + length;
            final long recordingId = recordingRange.recordingId;

            if (archivingNotComplete(endPosition, recordingId))
            {
                DebugLogger.log(logTag, "Archiving not complete");

                // Retry on the next iteration
                recordingRange = null;
                return false;
            }
            else
            {
                ranges.remove(0);
            }

            try
            {
                aeronSessionId = (int)aeronArchive.startReplay(
                    recordingId,
                    beginPosition,
                    length,
                    IPC_CHANNEL,
                    archiveReplayStream);

                messageTracker.reset();

                // reset the image if the new recordingRange requires it
                if (image != null && aeronSessionId != image.sessionId())
                {
                    image = null;
                }
            }
            catch (final Throwable exception)
            {
                errorHandler.onError(exception);

                return true;
            }
        }

        if (image == null)
        {
            DebugLogger.log(logTag, "Acquiring Replay Image");

            image = subscription.imageBySessionId(aeronSessionId);

            return false;
        }
        else
        {
            DebugLogger.log(logTag, "Polling Replay Image");

            image.controlledPoll(assembler, Integer.MAX_VALUE);

            // Have we finished this range?
            final int messageTrackerCount = messageTracker.count;
            final int recordingRangeCount = recordingRange.count;
            if (messageTrackerCount < recordingRangeCount)
            {
                return false;
            }
            else
            {
                DebugLogger.log(
                    logTag,
                    "Finished with range by count: [%s < %s]%n",
                    messageTrackerCount,
                    recordingRangeCount);

                replayedMessages += recordingRangeCount;
                recordingRange = null;

                return ranges.isEmpty();
            }
        }
    }

    int replayedMessages()
    {
        return replayedMessages;
    }

    private boolean archivingNotComplete(final long endPosition, final long recordingId)
    {
        final int counterId = RecordingPos.findCounterIdByRecording(countersReader, recordingId);

        // wait if the recording is active - otherwise assume that the recording has complete.
        if (counterId != CountersReader.NULL_COUNTER_ID)
        {
            return countersReader.getCounterValue(counterId) < endPosition;
        }

        return false;
    }

    private static class MessageTracker implements ControlledFragmentHandler
    {
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final FixMessageDecoder messageDecoder = new FixMessageDecoder();
        private final LogTag logTag;
        private final ControlledFragmentHandler messageHandler;

        int count;
        long sessionId;

        MessageTracker(final LogTag logTag, final ControlledFragmentHandler messageHandler)
        {
            this.logTag = logTag;
            this.messageHandler = messageHandler;
        }

        @Override
        public Action onFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageHeaderDecoder.wrap(buffer, offset);

            if (messageHeaderDecoder.templateId() == FixMessageDecoder.TEMPLATE_ID)
            {
                final int messageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
                if (sessionId != UNK_SESSION)
                {
                    messageDecoder.wrap(
                        buffer,
                        messageOffset,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version()
                    );

                    if (messageDecoder.session() != sessionId)
                    {
                        return CONTINUE;
                    }
                }

                if (DebugLogger.isEnabled(logTag))
                {
                    DebugLogger.log(logTag, "Found Replay Message [%s]%n", messageDecoder.body());
                }

                final Action action = messageHandler.onFragment(buffer, offset, length, header);
                if (action != ABORT)
                {
                    count++;
                }
                return action;
            }

            return CONTINUE;
        }

        void reset()
        {
            count = 0;
        }
    }
}
