package uk.co.real_logic.artio.admin;

import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.MappedFile;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;

import static uk.co.real_logic.artio.engine.ConnectedSessionInfo.UNK_SESSION;

/**
 * This example shows how to print out the state of index files stored by the engine. Currently
 * only supports sequence numbers.
 */
public final class EngineIndexPrinter
{

    public static void main(final String[] args)
    {
        for (final String engineLogDir : args)
        {
            final EngineConfiguration engineConfiguration = new EngineConfiguration()
                .logFileDir(engineLogDir)
                .libraryAeronChannel("")
                .conclude();

            final MappedFile receivedSequenceNumberIndex = engineConfiguration.receivedSequenceNumberIndex();

            System.out.printf("Inspecting %s%n", receivedSequenceNumberIndex.file().getAbsolutePath());

            final SequenceNumberIndexReader reader = new SequenceNumberIndexReader(
                receivedSequenceNumberIndex.buffer(), Throwable::printStackTrace,
                null, null);

            for (long sessionId = 0; sessionId < Long.MAX_VALUE; sessionId++)
            {
                final int sequenceNumber = reader.lastKnownSequenceNumber(sessionId);

                if (sequenceNumber == UNK_SESSION)
                {
                    break;
                }

                System.out.printf("Last seen sequence number for %d is %d%n", sessionId, sequenceNumber);
            }
        }
    }
}
