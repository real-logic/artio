package uk.co.real_logic.artio.engine;

import org.junit.Test;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static org.mockito.Mockito.*;

public class SoloPositionSenderTest
{
    private static final int LIBRARY_ID = 1;
    private static final int OTHER_LIBRARY_ID = 2;

    private GatewayPublication publication = mock(GatewayPublication.class);
    private SoloPositionSender positionSender = new SoloPositionSender(publication);

    @Test
    public void shouldSendUpdatedPositions()
    {
        positionSender.newPosition(LIBRARY_ID, 1024);
        positionSender.newPosition(LIBRARY_ID, 2048);
        positionSender.newPosition(OTHER_LIBRARY_ID, 768);

        doThreeWorks();

        verify(publication).saveNewSentPosition(LIBRARY_ID, 2048);
        verify(publication).saveNewSentPosition(OTHER_LIBRARY_ID, 768);
    }

    @Test
    public void shouldResendUpdatedPositionsWhenBackPressured()
    {
        resendScenario(times(2), BACK_PRESSURED);
    }

    @Test
    public void shouldNotResendWhenDisconnected()
    {
        resendScenario(times(1), CLOSED);
    }

    private void resendScenario(final VerificationMode times, final long saveResponse)
    {
        when(publication.saveNewSentPosition(LIBRARY_ID, 1024))
            .thenReturn(saveResponse, 100L);

        positionSender.newPosition(LIBRARY_ID, 1024);

        doThreeWorks();

        verify(publication, times)
            .saveNewSentPosition(LIBRARY_ID, 1024);
    }

    private void doThreeWorks()
    {
        positionSender.doWork();
        positionSender.doWork();
        positionSender.doWork();
    }
}
