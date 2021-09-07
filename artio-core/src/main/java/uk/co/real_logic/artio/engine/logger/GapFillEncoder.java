package uk.co.real_logic.artio.engine.logger;

import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.builder.AbstractSequenceResetEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.engine.HeaderSetup;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

class GapFillEncoder
{
    private static final int ENCODE_BUFFER_SIZE = 1024;

    private final AbstractSequenceResetEncoder sequenceResetEncoder;
    private final UtcTimestampEncoder timestampEncoder;
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);
    private final EpochNanoClock nanoClock;

    GapFillEncoder(
        final AbstractSequenceResetEncoder sequenceResetEncoder,
        final UtcTimestampEncoder timestampEncoder,
        final EpochNanoClock nanoClock)
    {
        this.sequenceResetEncoder = sequenceResetEncoder;
        this.timestampEncoder = timestampEncoder;
        this.nanoClock = nanoClock;
        this.sequenceResetEncoder.header().possDupFlag(true);
        this.sequenceResetEncoder.gapFillFlag(true);
    }

    long encode(final int msgSeqNum, final int newSeqNo)
    {
        final SessionHeaderEncoder respHeader = sequenceResetEncoder.header();
        respHeader.sendingTime(timestampEncoder.buffer(),
            timestampEncoder.encodeFrom(nanoClock.nanoTime(), TimeUnit.NANOSECONDS));
        respHeader.msgSeqNum(msgSeqNum);
        sequenceResetEncoder.newSeqNo(newSeqNo);

        return sequenceResetEncoder.encode(buffer, 0);
    }

    void setupMessage(final SessionHeaderDecoder requestHeader)
    {
        HeaderSetup.setup(requestHeader, sequenceResetEncoder.header());
    }

    MutableAsciiBuffer buffer()
    {
        return buffer;
    }
}
