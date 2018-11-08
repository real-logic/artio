package uk.co.real_logic.artio.util;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AsciiSequenceViewTest
{
    private static final int INDEX = 2;
    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[128]);
    private final AsciiSequenceView asciiSequenceView = new AsciiSequenceView();

    @Test
    public void shouldBeAbleToGetChars()
    {
        final String data = "stringy";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        asciiSequenceView.wrap(buffer, INDEX, data.length());

        assertThat(asciiSequenceView.charAt(0), is('s'));
        assertThat(asciiSequenceView.charAt(1), is('t'));
        assertThat(asciiSequenceView.charAt(2), is('r'));
        assertThat(asciiSequenceView.charAt(3), is('i'));
        assertThat(asciiSequenceView.charAt(4), is('n'));
        assertThat(asciiSequenceView.charAt(5), is('g'));
        assertThat(asciiSequenceView.charAt(6), is('y'));
    }

    @Test
    public void shouldToString()
    {
        final String data = "a little bit of ascii";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        asciiSequenceView.wrap(buffer, INDEX, data.length());

        assertThat(asciiSequenceView.toString(), is(data));
    }

    @Test
    public void shouldReturnCorrectLength()
    {
        final String data = "a little bit of ascii";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        asciiSequenceView.wrap(buffer, INDEX, data.length());

        assertThat(asciiSequenceView.length(), is(data.length()));
    }

    @Test
    public void shouldCopyDataUnderTheView()
    {
        final String data = "a little bit of ascii";
        final int targetBufferOffset = 56;
        final MutableDirectBuffer targetBuffer = new UnsafeBuffer(new byte[128]);
        buffer.putStringWithoutLengthAscii(INDEX, data);
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        asciiSequenceView.getBytes(targetBuffer, targetBufferOffset);

        assertThat(targetBuffer.getStringWithoutLengthAscii(targetBufferOffset, data.length()), is(data));
    }

    @Test
    public void shouldSubSequence()
    {
        final String data = "a little bit of ascii";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        asciiSequenceView.wrap(buffer, INDEX, data.length());
        final AsciiSequenceView subSequenceView = asciiSequenceView.subSequence(2, 8);

        assertThat(subSequenceView.toString(), is("little"));
    }

    @Test
    public void shouldReturnEmptyStringWhenBufferIsNull()
    {
        assertEquals(0, asciiSequenceView.length());
        assertEquals("", asciiSequenceView.toString());
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void shouldThrowIndexOutOfBoundsExceptionWhenCharNotPresentAtGivenPosition()
    {
        final String data = "foo";
        buffer.putStringWithoutLengthAscii(INDEX, data);
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        asciiSequenceView.charAt(4);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void shouldThrowExceptionWhenCharAtCalledWithNoBuffer()
    {
        asciiSequenceView.charAt(0);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void shouldThrowExceptionWhenCharAtCalledWithNegativeIndex()
    {
        final String data = "foo";
        buffer.putStringWithoutLengthAscii(INDEX, data);
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        asciiSequenceView.charAt(-1);
    }
}