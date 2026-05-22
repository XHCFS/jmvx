package edu.uic.cs.jmvx.runtime.pools;

import java.util.Comparator;

public interface JMVXOrderedEvent extends Comparator<JMVXOrderedEvent>, Comparable<JMVXOrderedEvent> {
    public void $JMVX$setClock(int newClock);
    public int $JMVX$getClock();

    @Override
    default int compareTo(JMVXOrderedEvent o){
        return compare(this, o);
    }

    @Override
    default int compare(JMVXOrderedEvent o1, JMVXOrderedEvent o2){
        return Comparator.comparingInt(JMVXOrderedEvent::$JMVX$getClock).compare(o1, o2);
    }
}
