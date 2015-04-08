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
package uk.co.real_logic.fix_gateway.session;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SenderAndTargetSessionIdStrategyTest
{
    private static List<char[]> identifiers = new ArrayList<>();

    private SenderAndTargetSessionIdStrategy strategy = new SenderAndTargetSessionIdStrategy();

    @BeforeClass
    public static void generateIdentifiers()
    {
        IntStream.range(0, 100)
                 .mapToObj(i -> UUID.randomUUID().toString().toCharArray())
                 .forEach(identifiers::add);

        identifiers.add("SIGMAX".toCharArray());
        identifiers.add("ABC_DEFG04".toCharArray());
        identifiers.add("ABC_DEFG01".toCharArray());
        identifiers.add("CCG".toCharArray());
        identifiers.add("LEH_LZJ02".toCharArray());
    }

    @Test
    public void differentSessionsGenerateDifferentIds()
    {
        List<Long> ids = decodeIds();

        new HashSet<>(ids).forEach(ids::remove);

        assertEquals("You have duplicate ids", Collections.<Long>emptyList(), ids);
    }

    @Test
    public void theSameSessionGeneratesTheSameId()
    {
        List<Long> firstIds = decodeIds();
        List<Long> secondIds = decodeIds();

        assertEquals("The ids aren't generated equally on future runs", firstIds, secondIds);
    }

    @Test
    public void shouldEncodeFieldsAsRegisteredWith()
    {
        final List<Long> ids = decodeIds();

        final int size = identifiers.size();
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                final HeaderEncoder mockHeader = mock(HeaderEncoder.class);
                strategy.encode(ids.get(i * size+ j), mockHeader);
                verify(mockHeader).senderCompID(identifiers.get(i));
                verify(mockHeader).targetCompID(identifiers.get(j));
            }
        }
    }

    private List<Long> decodeIds()
    {
        return identifiers.stream()
                          .flatMap(senderId -> identifiers.stream()
                              .map(targetId -> strategy.register(senderId, targetId)))
                          .collect(toList());
    }

}
