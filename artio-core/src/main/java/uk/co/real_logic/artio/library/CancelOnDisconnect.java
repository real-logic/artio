package uk.co.real_logic.artio.library;

import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.messages.CancelOnDisconnectOption;

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongPredicate;

import static uk.co.real_logic.artio.messages.CancelOnDisconnectOption.*;

public final class CancelOnDisconnect
{
    private Consumer<BooleanSupplier> enqueueTask;
    private final EpochNanoClock clock;
    private final boolean isAcceptor;
    private final LongPredicate sendCancelOnDisconnectTrigger;

    private long cancelOnDisconnectTimeoutWindowInNs;
    private CancelOnDisconnectOption cancelOnDisconnectOption;

    public CancelOnDisconnect(
        final EpochNanoClock clock,
        final boolean isAcceptor,
        final LongPredicate sendCancelOnDisconnectTrigger)
    {
        this.clock = clock;
        this.isAcceptor = isAcceptor;
        this.sendCancelOnDisconnectTrigger = sendCancelOnDisconnectTrigger;
    }

    public void enqueueTask(final Consumer<BooleanSupplier> enqueueTask)
    {
        this.enqueueTask = enqueueTask;
    }

    public void checkCancelOnDisconnectLogout(final long timeInNs)
    {
        if (!notifyCancelOnDisconnect(timeInNs, CANCEL_ON_LOGOUT_ONLY))
        {
            enqueueTask.accept(() -> notifyCancelOnDisconnect(timeInNs, CANCEL_ON_LOGOUT_ONLY));
        }
    }

    public void checkCancelOnDisconnectDisconnect()
    {
        final long timeInNs = clock.nanoTime();
        if (!notifyCancelOnDisconnect(timeInNs, CANCEL_ON_DISCONNECT_ONLY))
        {
            enqueueTask.accept(() -> notifyCancelOnDisconnect(timeInNs, CANCEL_ON_DISCONNECT_ONLY));
        }
    }

    private boolean notifyCancelOnDisconnect(final long timeInNs, final CancelOnDisconnectOption option)
    {
        if (isAcceptor && (cancelOnDisconnectOption == option ||
            cancelOnDisconnectOption == CANCEL_ON_DISCONNECT_OR_LOGOUT))
        {
            final long deadlineInNs = timeInNs + cancelOnDisconnectTimeoutWindowInNs;
            return sendCancelOnDisconnectTrigger.test(deadlineInNs);
        }

        return true;
    }

    public void cancelOnDisconnectTimeoutWindowInNs(final long cancelOnDisconnectTimeoutWindowInNs)
    {
        this.cancelOnDisconnectTimeoutWindowInNs = cancelOnDisconnectTimeoutWindowInNs;
    }

    public void cancelOnDisconnectOption(final CancelOnDisconnectOption cancelOnDisconnectOption)
    {
        this.cancelOnDisconnectOption = cancelOnDisconnectOption;
    }
}
