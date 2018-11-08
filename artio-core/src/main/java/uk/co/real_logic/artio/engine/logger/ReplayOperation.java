/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.GatewayProcess.ARCHIVE_REPLAY_STREAM;

/**
 * A continuable replay operation that can retried
 */
public class ReplayOperation
{
    private final MessageTracker messageTracker = new MessageTracker();
    private final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(messageTracker);

    private final List<RecordingRange> ranges;
    private final AeronArchive aeronArchive;
    private final ErrorHandler errorHandler;
    private final CountersReader countersReader;
    private final Subscription subscription;

    private int replayedMessages = 0;
    private RecordingRange recordingRange;




    ReplayOperation(
        final ControlledFragmentHandler handler,
        final List<RecordingRange> ranges,
        final AeronArchive aeronArchive,
        final ErrorHandler errorHandler)
    {
        this.ranges = ranges;
        this.aeronArchive = aeronArchive;
        this.errorHandler = errorHandler;

        final Aeron aeron = aeronArchive.context().aeron();
        countersReader = aeron.countersReader();
        messageTracker.wrap(handler);
        subscription = aeron.addSubscription(IPC_CHANNEL, ARCHIVE_REPLAY_STREAM);
    }

    public boolean attemptReplay()
    {
        final boolean complete = attemptReplayStep();
        if (complete)
        {
            subscription.close();
        }
        return complete;
    }

    private boolean attemptReplayStep()
    {
        if (recordingRange == null)
        {
            if (ranges.isEmpty())
            {
                return true;
            }

            recordingRange = ranges.remove(0);

            final long beginPosition = recordingRange.position;
            final long length = recordingRange.length;
            final long endPosition = beginPosition + length;
            final long recordingId = recordingRange.recordingId;

            if (archivingNotComplete(endPosition, recordingId))
            {
                return false;
            }

            try
            {
                final int aeronSessionId = (int)aeronArchive.startReplay(
                    recordingId,
                    beginPosition,
                    length,
                    IPC_CHANNEL,
                    ARCHIVE_REPLAY_STREAM);

                messageTracker.reset(aeronSessionId);
            }
            catch (final Throwable exception)
            {
                errorHandler.onError(exception);

                return true;
            }
        }

        subscription.controlledPoll(assembler, Integer.MAX_VALUE);

        // Have we finished this range?
        if (messageTracker.count < recordingRange.count)
        {
            return false;
        }
        else
        {
            replayedMessages += recordingRange.count;
            recordingRange = null;

            return ranges.isEmpty();
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

        ControlledFragmentHandler messageHandler;
        int count;
        private int aeronSessionId;

        @Override
        public Action onFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (header.sessionId() == aeronSessionId)
            {
                messageHeaderDecoder.wrap(buffer, offset);

                if (messageHeaderDecoder.templateId() == FixMessageDecoder.TEMPLATE_ID)
                {
                    final Action action = messageHandler.onFragment(buffer, offset, length, header);
                    if (action != ABORT)
                    {
                        count++;
                    }
                    return action;
                }
            }

            return CONTINUE;
        }

        void wrap(final ControlledFragmentHandler handler)
        {
            this.messageHandler = handler;
        }

        void reset(final int aeronSessionId)
        {
            count = 0;
            this.aeronSessionId = aeronSessionId;
        }
    }
}
