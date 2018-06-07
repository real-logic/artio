/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CodecUtilTest
{

    @Test
    public void shouldCheckSubsectionOfCharArrays()
    {
        assertTrue(CodecUtil.equals("abc".toCharArray(), "abc    ".toCharArray(), 0, 0, 3));
    }

    @Test
    public void shouldSupportShortArrays()
    {
        assertFalse(CodecUtil.equals("abc".toCharArray(), "ab".toCharArray(), 0, 0, 3));
    }

    @Test
    public void shouldFindDifferentCharacters()
    {
        assertFalse(CodecUtil.equals("abc".toCharArray(), "azc    ".toCharArray(), 0, 0, 3));
    }

    @Test
    public void shouldFindDifferentCharactersWithOffset()
    {
        assertFalse(CodecUtil.equals("zyxabc".toCharArray(), "azc    ".toCharArray(), 3, 0, 3));
    }

    @Test
    public void shouldCheckSubsectionOfCharArraysWithOffset()
    {
        assertTrue(CodecUtil.equals("zyxabc".toCharArray(), "abc    ".toCharArray(), 3, 0, 3));
    }

    @Test
    public void testHashCodeWithOffset()
    {
        final int firstHash = CodecUtil.hashCode("zyxabc".toCharArray(), 0, 3);
        final int secondHash = CodecUtil.hashCode("abczyx".toCharArray(), 3, 3);
        assertEquals(firstHash, secondHash);
    }
}
