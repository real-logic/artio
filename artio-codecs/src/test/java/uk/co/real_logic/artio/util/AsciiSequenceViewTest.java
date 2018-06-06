package uk.co.real_logic.artio.util;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AsciiSequenceViewTest
{
    private static final int INDEX = 2;
    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[128]);
    private final AsciiSequenceView asciiSequenceView = new AsciiSequenceView();

    @Test
    public void shouldBeAbleToGetChars()
    {
        //Given
        final String data = "stringy";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        //When
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        //Then
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
        //Given
        final String data = "a little bit of ascii";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        //When
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        //Then
        assertThat(asciiSequenceView.toString(), is(data));
    }

    @Test
    public void shouldReturnCorrectLength()
    {
        //Given
        final String data = "a little bit of ascii";
        buffer.putStringWithoutLengthAscii(INDEX, data);

        //When
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        //Then
        assertThat(asciiSequenceView.length(), is(data.length()));
    }

    @Test
    public void shouldCopyDataUnderTheView()
    {
        //Given
        final String data = "a little bit of ascii";
        final int targetBufferOffset = 56;
        final MutableDirectBuffer targetBuffer = new UnsafeBuffer(new byte[128]);
        buffer.putStringWithoutLengthAscii(INDEX, data);
        asciiSequenceView.wrap(buffer, INDEX, data.length());

        //When
        asciiSequenceView.getBytes(targetBuffer, targetBufferOffset);

        //Then
        assertThat(targetBuffer.getStringWithoutLengthAscii(targetBufferOffset, data.length()), is(data));
    }
}