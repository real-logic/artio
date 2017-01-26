/*
 * Copyright 2015-2016 Real Logic Ltd.
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


import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.sbe_util.MessageDumper;
import uk.co.real_logic.fix_gateway.sbe_util.MessageSchemaIr;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEBUG_TAGS;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEBUG_FILE;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEBUG_PRINT_MESSAGES;

/**
 * A logger purely for debug data. Not optimised for high performance logging, but all logging calls must be removable
 * by the optimiser.
 */
public final class DebugLogger
{
    private static final PrintStream OUTPUT;

    static
    {
        if (DEBUG_FILE == null)
        {
            OUTPUT = System.out;
        }
        else
        {
            PrintStream output = null;
            try
            {
                output = new PrintStream(new FileOutputStream(DEBUG_FILE));
            }
            catch (IOException e)
            {
                e.printStackTrace();
                System.exit(-1);
            }
            finally
            {
                OUTPUT = output;
            }
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final int value,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            log(tag, formatString, Integer.valueOf(value), buffer, offset, length);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object value,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            printf(formatString, value, new String(data, US_ASCII));
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final DirectBuffer buffer,
        final int offset)
    {
        if (isEnabled(tag))
        {
            println(toStringSbeMessage(buffer, offset));
        }
    }

    public static String toStringSbeMessage(final DirectBuffer buffer, final int offset)
    {
        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        headerDecoder.wrap(buffer, offset);
        final MessageDumper dumper = new MessageDumper(MessageSchemaIr.SCHEMA_BUFFER);
        return dumper.toString(
            headerDecoder.templateId(),
            headerDecoder.version(),
            headerDecoder.blockLength(),
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH
        );
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            printf(formatString, new String(data, US_ASCII));
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final ByteBuffer byteBuffer,
        final int length)
    {
        if (isEnabled(tag))
        {
            final byte[] data = new byte[length];
            final int originalPosition = byteBuffer.position();
            byteBuffer.position(originalPosition - length);
            byteBuffer.get(data);
            byteBuffer.position(originalPosition);

            printf(formatString, new String(data, US_ASCII));
        }
    }

    public static void log(
        final LogTag tag,
        final String message)
    {
        if (isEnabled(tag))
        {
            println(message);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object value)
    {
        if (isEnabled(tag))
        {
            printf(formatString, value);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final boolean second)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final Object second)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second,
        final long third)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final long second,
        final long third)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second,
        final long third,
        final long fourth)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final long second,
        final long third,
        final long fourth)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth);
        }
    }

    private static void printf(
        final String formatString,
        final Object ... args)
    {
        OUTPUT.printf(threadName() + formatString, args);
    }

    private static void println(final String message)
    {
        OUTPUT.println(threadName() + message);
    }

    private static String threadName()
    {
        return Thread.currentThread().getName() + " : ";
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final Object second,
        final Object third)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second,
        final long third,
        final long fourth,
        final long fifth)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth, fifth);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second,
        final long third,
        final long fourth,
        final long fifth,
        final long sixth)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth, fifth, sixth);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final long second,
        final long third,
        final long fourth,
        final long fifth,
        final long sixth,
        final long seventh)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth, fifth, sixth, seventh);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final long second,
        final long third,
        final long fourth,
        final long fifth)
    {
        if (isEnabled(tag))
        {
            printf(formatString, first, second, third, fourth, fifth);
        }
    }

    private static boolean isEnabled(final LogTag tag)
    {
        return DEBUG_PRINT_MESSAGES && DEBUG_TAGS.contains(tag);
    }

}
