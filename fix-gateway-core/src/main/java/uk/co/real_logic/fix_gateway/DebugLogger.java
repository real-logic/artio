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


import uk.co.real_logic.agrona.DirectBuffer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
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
        final String formatString, final Object value, final DirectBuffer buffer, final int offset, final int length)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            OUTPUT.printf(formatString, value, new String(data, US_ASCII));
        }
    }

    public static void log(final String formatString, final DirectBuffer buffer, final int offset, final int length)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            OUTPUT.printf(formatString, new String(data, US_ASCII));
        }
    }

    public static void log(final String formatString, final ByteBuffer byteBuffer, final int length)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            final byte[] data = new byte[length];
            final int originalPosition = byteBuffer.position();
            byteBuffer.position(originalPosition - length);
            byteBuffer.get(data);
            byteBuffer.position(originalPosition);

            OUTPUT.printf(formatString, new String(data, US_ASCII));
        }
    }

    public static void log(final String message)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.println(message);
        }
    }

    public static void log(final String formatString, final Object value)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, value);
        }
    }

    public static void log(final String formatString, final long first)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, first);
        }
    }

    public static void log(final String formatString, final Object first, final Object second)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, first, second);
        }
    }

    public static void log(final String formatString, final long first, final long second)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, first, second);
        }
    }

    public static void log(final String formatString, final long first, final long second, final long third)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, first, second, third);
        }
    }

    public static void log(final String formatString, final Object first, final Object second, final Object third)
    {
        if (DEBUG_PRINT_MESSAGES)
        {
            OUTPUT.printf(formatString, first, second, third);
        }
    }
}
