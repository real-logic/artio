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
package uk.co.real_logic.artio;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.util.MessageTypeEncoding.packMessageType;

public class BusinessRejectRefIdExtractorTest
{
    @Test
    public void shouldExtractBusinessRejectRefId()
    {
        final String msg = "8=FIX.4.4\0019=154\00135=6\00149=FOO\00156=BAR\00134=2\00152=20200505-09:48:58" +
            "\00123=224553\00128=N\00155=BAZ\00154=1\00127=3000\00144=22333.000000\00125=H\00110=170\001";

        final UnsafeBuffer buffer = new UnsafeBuffer(msg.getBytes(StandardCharsets.US_ASCII));

        final BusinessRejectRefIdExtractor extractor = new BusinessRejectRefIdExtractor();
        extractor.search(packMessageType("6"), buffer, 0, buffer.capacity());

        assertEquals("224553", extractor.buffer().getAscii(extractor.offset(), extractor.length()));
    }
}
