package uk.co.real_logic.artio.dictionary;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CharArrayMapTest
{
    @Test
    public void shouldReturnTrueIfKeyIsPresent()
    {
        //Given
        final Map<String, String> buildFrom = new HashMap<>();
        buildFrom.put("0", "_0");
        buildFrom.put("A", "_A");
        buildFrom.put("AA", "_AAA");
        final CharArrayMap<String> charArrayMap = new CharArrayMap<>(buildFrom);

        //When / Then
        assertTrue(charArrayMap.containsKey(new char[] {'0', ' ', 'A', 'A'}, 0, 1));
        assertTrue(charArrayMap.containsKey(new char[] {'0', ' ', 'A', 'A'}, 2, 2));
    }

    @Test
    public void shouldReturnFalseIfKeyIsNotPresent()
    {
        //Given
        final Map<String, String> buildFrom = new HashMap<>();
        buildFrom.put("0", "_0");
        buildFrom.put("A", "_A");
        buildFrom.put("AA", "_AAA");
        final CharArrayMap<String> charArrayMap = new CharArrayMap<>(buildFrom);

        //When / Then
        assertFalse(charArrayMap.containsKey(new char[] {'0', ' ', 'A', 'B'}, 2, 2));
    }
}