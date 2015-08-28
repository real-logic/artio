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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.RecordBuffer;
import uk.co.real_logic.fix_gateway.messages.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static uk.co.real_logic.agrona.concurrent.RecordBuffer.DID_NOT_CLAIM_RECORD;

/**
 * A buffer which keeps track of the last exception thrown from a callsite.
 *
 * Instances are single threaded, but buffers are threadsafe to read from another thread.
 *
 * NB: its possible in a multi-threaded scenario to log two exceptions with the same hash,
 * however, exceptions are still bounded in worst case by number of threads * number of hashes.
 */
public class ErrorBuffer implements ErrorHandler
{
    private static final int STACK_TRACE_ELEMENT_MIN = StackTraceElementEncoder.BLOCK_LENGTH + 6;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ExceptionEntryEncoder exceptionEntryEncoder = new ExceptionEntryEncoder();
    private final ExceptionEntryDecoder exceptionDecoder = new ExceptionEntryDecoder();
    private final StackTraceElementEncoder stackTraceElementEncoder = new StackTraceElementEncoder();
    private final StackTraceElementDecoder stackTraceElementDecoder = new StackTraceElementDecoder();
    private final int exceptionBlockLength = exceptionEntryEncoder.sbeBlockLength();
    private final int exceptionVersion = exceptionEntryEncoder.sbeSchemaVersion();
    private final int elementBlockLength = stackTraceElementEncoder.sbeBlockLength();
    private final int elementVersion = stackTraceElementEncoder.sbeSchemaVersion();
    private final RecordBuffer.RecordHandler onErrorFunc = (key, offset) -> onError(offset);

    private final AtomicBuffer buffer;
    private final RecordBuffer recordBuffer;
    private final AtomicCounter counter;
    private final EpochClock clock;
    private final int maxRecordSize;

    private List<String> errors;
    private long timeInMillis;

    /**
     * Read only constructor.
     *
     * @param buffer the buffer to use for outputting errors on.
     */
    public ErrorBuffer(final AtomicBuffer buffer, final int maxRecordSize)
    {
        this(buffer, null, null, maxRecordSize);
    }

    public ErrorBuffer(
        final AtomicBuffer buffer, final AtomicCounter counter, final EpochClock clock, final int maxRecordSize)
    {
        this.buffer = buffer;
        this.counter = counter;
        this.clock = clock;
        this.maxRecordSize = maxRecordSize;
        recordBuffer = new RecordBuffer(buffer, MessageHeaderEncoder.ENCODED_LENGTH, maxRecordSize);

        setupBuffers(buffer);
    }

    private void setupBuffers(final AtomicBuffer buffer)
    {
        messageHeaderDecoder.wrap(buffer, 0);
        messageHeaderEncoder.wrap(buffer, 0);

        if (isFreshBuffer())
        {
            writeHeader();
        }
        else
        {
            validateHeader();
        }
    }

    private void validateHeader()
    {
        validate(messageHeaderDecoder.schemaId(), exceptionEntryEncoder.sbeSchemaId(), "Schema Id");
        validate(messageHeaderDecoder.version(), exceptionEntryEncoder.sbeSchemaVersion(), "Schema Version");
    }

    private void writeHeader()
    {
        messageHeaderEncoder
            .blockLength(exceptionBlockLength)
            .templateId(exceptionEntryEncoder.sbeTemplateId())
            .schemaId(exceptionEntryEncoder.sbeSchemaId())
            .version(exceptionVersion);

        recordBuffer.initialise();
    }

    private void validate(final int read, final int expected, final String name)
    {
        if (read != expected)
        {
            throw new IllegalStateException(
                String.format("Wrong %s, expected %d, but was %d", name, read, expected));
        }
    }

    private boolean isFreshBuffer()
    {
        return messageHeaderDecoder.schemaId() == 0;
    }

    /**
     * Read out the current list of errors that have been saved.
     *
     * @return a list of errors, each exception represented as a string.
     */
    public List<String> errors()
    {
        return errorsSince(0L);
    }

    public List<String> errorsSince(final long timeInMillis)
    {
        this.timeInMillis = timeInMillis;
        recordBuffer.forEach(onErrorFunc);
        return errors != null ? errors : emptyList();
    }

    private void onError(final int offset)
    {
        wrapExceptionDecoder(offset);
        if (exceptionDecoder.time() > timeInMillis)
        {
            final StringBuilder builder = new StringBuilder();
            appendException(builder);
            appendStackTraceElements(exceptionDecoder.limit(), builder);
            if (errors == null)
            {
                errors = new ArrayList<>();
            }
            errors.add(builder.toString());
        }
    }

    private void appendStackTraceElements(int offset, final StringBuilder builder)
    {
        final int stackTraceSize = exceptionDecoder.elementCount();
        for (int i = 0; i < stackTraceSize; i++)
        {
            stackTraceElementDecoder.wrap(buffer, offset, elementBlockLength, elementVersion);

            appendStackTraceElement(builder);

            offset = stackTraceElementDecoder.limit();
        }
    }

    private void appendStackTraceElement(final StringBuilder builder)
    {
        builder.append(String.format(
            "\n%s.%s(%s:%d)",
            stackTraceElementDecoder.className(),
            stackTraceElementDecoder.methodName(),
            stackTraceElementDecoder.fileName(),
            stackTraceElementDecoder.lineNumber()));
    }

    private void appendException(final StringBuilder builder)
    {
        builder.append(formatTimeStamp());
        builder.append(": ");
        builder.append(exceptionDecoder.exceptionClassName());
        builder.append("(");
        builder.append(exceptionDecoder.message());
        builder.append(")");
    }

    private void wrapExceptionDecoder(final int offset)
    {
        exceptionDecoder.wrap(buffer, offset, exceptionBlockLength, exceptionVersion);
    }

    private String formatTimeStamp()
    {
        final Instant instant = Instant.ofEpochMilli(exceptionDecoder.time());
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toString();
    }

    /**
     * {@inheritDoc}
     */
    public void onError(final Throwable ex)
    {
        counter.orderedIncrement();

        final StackTraceElement[] stackTrace = ex.getStackTrace();
        final int hash = hashThrowSite(stackTrace[0]);
        final String message = ex.getMessage() != null ? ex.getMessage() : "No Message";
        final String exceptionName = ex.getClass().getName();

        final int claimedOffset = recordBuffer.claimRecord(hash);
        if (claimedOffset == DID_NOT_CLAIM_RECORD)
        {
            return;
        }

        final int claimedLimit = claimedOffset + maxRecordSize;
        int offset = claimedOffset;

        exceptionEntryEncoder
            .wrap(buffer, offset)
            .hash(hash)
            .time(clock.time())
            .exceptionClassName(exceptionName)
            .message(message);

        offset = exceptionEntryEncoder.limit();

        int i = 0;
        for (; i < stackTrace.length; i++)
        {
            final StackTraceElement element = stackTrace[i];
            if ((offset + sizeOfElement(element)) < claimedLimit)
            {
                stackTraceElementEncoder
                    .wrap(buffer, offset)
                    .lineNumber(element.getLineNumber())
                    .className(element.getClassName())
                    .methodName(element.getMethodName())
                    .fileName(element.getFileName());

                offset = stackTraceElementEncoder.limit();
            }
            else
            {
                break;
            }
        }
        exceptionEntryEncoder.elementCount((byte) i);

        recordBuffer.commit(claimedOffset);

        if (claimedLimit < offset)
        {
            System.err.printf(
                "Unexpected offset logging errors, claimedOffset = %d, size = %d, offset = %d\n",
                claimedOffset,
                maxRecordSize,
                offset);
        }
    }

    private int sizeOfElement(final StackTraceElement element)
    {
        return STACK_TRACE_ELEMENT_MIN +
               element.getClassName().length() +
               element.getMethodName().length() +
               element.getFileName().length();
    }

    private int hashThrowSite(final StackTraceElement throwSite)
    {
        return throwSite.getClassName().hashCode() + throwSite.getLineNumber() * 31;
    }
}
