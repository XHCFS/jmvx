package edu.uic.cs.jmvx.runtime;

import edu.uic.cs.jmvx.runtime.strategy.Follower;
import edu.uic.cs.jmvx.runtime.strategy.HandlerStatus;
import edu.uic.cs.jmvx.runtime.strategy.Metadata;

import java.util.Arrays;
import java.util.List;

public class MicrobenchDivergenceHandler implements DivergenceHandler{

    @Override
    public HandlerStatus<Integer> handleWriteDivergence(List<StackTraceElement> stack, Metadata metadata) {
        /*
        Don't add message. Leaving this here to highlight a weakness of the divergence handler
        Dacapo prints out a large string all at once with println, whereas the micro benchmark used format
        to construct the string.
        Format leads to multiple print calls. The divergence occurs on a print of JUST THE NUMBER.
        We end up with out:
        "Duration: 3000 and divergence handled msec" rather than the desired
        "Duration: 3000 msec and divergence handled"
         */
        //DacapoDivergenceHandler.addDivergenceMessage(metadata);
        StackTraceElement[] trace = new Exception().getStackTrace();
        DivergenceStatus.STATUS status = Arrays.stream(trace)
                .anyMatch((frame) -> frame.getMethodName().equals("finish")) ?
                DivergenceStatus.STATUS.OK : DivergenceStatus.STATUS.NOT_OK;
        return new HandlerStatus<>(status, 0);
    }

    @Override
    public HandlerStatus<Integer> handleDivergence(List<StackTraceElement> stack) {
        return DivergenceHandler.super.handleDivergence(stack);
    }
}
