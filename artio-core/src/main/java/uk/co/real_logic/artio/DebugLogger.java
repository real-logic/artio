/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio;


import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.engine.ByteBufferUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static uk.co.real_logic.artio.CommonConfiguration.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEBUG_FILE;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEBUG_PRINT_MESSAGES;

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
            catch (final IOException ex)
            {
                ex.printStackTrace();
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
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            substituteSeparator(data);
            printf(tag, formatString, Integer.valueOf(value), new String(data, US_ASCII));
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final Object sbeObject)
    {
        if (isEnabled(tag))
        {
            println(sbeObject.toString());
        }
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
            substituteSeparator(data);
            printf(tag, formatString, new String(data, US_ASCII));
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
            ByteBufferUtil.position(byteBuffer, originalPosition - length);
            byteBuffer.get(data);
            ByteBufferUtil.position(byteBuffer, originalPosition);

            substituteSeparator(data);
            printf(tag, formatString, new String(data, US_ASCII));
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
            printf(tag, formatString, value);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first)
    {
        if (isEnabled(tag))
        {
            printf(tag, formatString, first);
        }
    }

    public static void log(
        final LogTag tag,
        final String formatString,
        final long first,
        final Object second)
    {
        if (isEnabled(tag))
        {
            printf(tag, formatString, first, second);
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
            printf(tag, formatString, first, second);
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
            printf(tag, formatString, first, second, third);
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
            printf(tag, formatString, first, second, third);
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
            printf(tag, formatString, first, second, third, fourth);
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
            printf(tag, formatString, first, second, third, fourth);
        }
    }

    // Used by fix-integration project
    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final Object second)
    {
        if (isEnabled(tag))
        {
            printf(tag, formatString, first, second);
        }
    }

    private static void printf(
        final LogTag tag,
        final String formatString,
        final Object... args)
    {
        final String threadName = threadName();
        if (isThreadEnabled(threadName))
        {
            OUTPUT.printf(System.currentTimeMillis() + ":" +
                threadName + "[" + tag.name() + "]" + " : " + formatString, args);
        }
    }

    private static void println(final String message)
    {
        final String threadName = threadName();
        if (isThreadEnabled(threadName))
        {
            OUTPUT.println(threadName + message);
        }
    }

    private static String threadName()
    {
        return Thread.currentThread().getName();
    }

    private static void substituteSeparator(final byte[] data)
    {
        if (DEBUG_LOGGING_SEPARATOR != DEFAULT_DEBUG_LOGGING_SEPARATOR)
        {
            final int size = data.length;
            for (int i = 0; i < size; i++)
            {
                if (data[i] == DEFAULT_DEBUG_LOGGING_SEPARATOR)
                {
                    data[i] = DEBUG_LOGGING_SEPARATOR;
                }
            }
        }
    }

    public static boolean isEnabled(final LogTag tag)
    {
        return DEBUG_PRINT_MESSAGES && DEBUG_TAGS.contains(tag);
    }

    private static boolean isThreadEnabled(final String threadName)
    {
        return DEBUG_PRINT_THREAD == null || DEBUG_PRINT_THREAD.equals(threadName);
    }
}
