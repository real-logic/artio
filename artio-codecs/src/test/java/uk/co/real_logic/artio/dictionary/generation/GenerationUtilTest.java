/*
 * Copyright 2013 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import java.util.BitSet;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.constantName;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.packMessageType;

public class GenerationUtilTest
{

    @Test
    public void shouldGenerateDifferentMessageTypeIdentifiers()
    {
        assertNotEquals(packMessageType("AL"), packMessageType("AM"));
        assertNotEquals(packMessageType("AL"), packMessageType("AN"));
        assertNotEquals(packMessageType("AR"), packMessageType("AQ"));
        assertNotEquals(packMessageType("BC"), packMessageType("BB"));
        assertNotEquals(packMessageType("BD"), packMessageType("BE"));
        assertNotEquals(packMessageType("BG"), packMessageType("BF"));
    }

    @Test
    public void shouldReturnSentinelValueForInvalidMsgType()
    {
        assertEquals(GenerationUtil.INVALID_MSG_TYPE, packMessageType("*"));
    }

    @Test
    public void generatesUniquePackedIds()
    {
        final BitSet seen = new BitSet();
        permutations(new StringBuilder(), 5, 0, seen);
    }

    @Test(expected = IllegalArgumentException.class)
    public void failsToPackMessageTypeFor6Characters()
    {
        packMessageType("AAAAAA");
    }

    @Test
    public void shouldReplaceIDWithIdInConstantName()
    {
        assertEquals("DEFAULT_APPL_VER_ID", constantName("DefaultApplVerID"));
    }

    @Test
    public void shouldDropGroupCounterForNumberOfElementsInReaptingGroupConstant()
    {
        assertEquals("NO_MSG_TYPES", constantName("NoMsgTypesGroupCounter"));
    }

    private static void permutations(
        final StringBuilder permutation, final int maxDepth,
        final int curDepth, final BitSet seen)
    {
        if (permutation.length() > 0)
        {
            final int packed = packMessageType(permutation);
            sawOrNot(permutation, seen, packed);
        }

        if (curDepth == maxDepth)
        {
            return;
        }
        for (int c = 48; c <= 122; c++)
        {
            switch (c)
            {
                case 58:
                case 59:
                case 60:
                case 61:
                case 62:
                case 63:
                case 64:
                case 91:
                case 92:
                case 93:
                case 94:
                case 95:
                case 96:
                    continue;
            }
            permutation.append((char)c);

            permutations(permutation, maxDepth, curDepth + 1, seen);
            permutation.setLength(permutation.length() - 1);
        }
    }

    private static void sawOrNot(final StringBuilder permutation, final BitSet seen, final int packed)
    {
        final boolean alreadySaw = seen.get(packed);
        if (alreadySaw)
        {
            Assert.assertFalse(permutation.toString(), alreadySaw);
        }
        seen.set(packed);
    }
}
