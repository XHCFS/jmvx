package edu.uic.cs.jmvx.runtime.strategy;

import edu.uic.cs.jmvx.runtime.pools.JMVXJob;
import edu.uic.cs.jmvx.runtime.pools.JMVXQueueingFuture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Only use a strategy for synchronization operations
 * @param <strategy>
 */
public class SyncOnly<strategy extends JMVXStrategy> extends ReentrantStrategy<strategy, SingleLeaderWithoutCoordinator>{


    public SyncOnly(strategy strategy) {
        super(strategy, new SingleLeaderWithoutCoordinator());
    }

    private boolean enabled = false;

    private void enable(){
        enabled = true;
    }

    private void disable(){
        enabled = false;
    }

    @Override
    public void monitorenter(Object o) {
        try {
            enable();
            super.monitorenter(o);
        }finally {
            disable();
        }
    }

    @Override
    public void monitorexit(Object o) {
        try {
            enable();
            super.monitorexit(o);
        }finally {
            disable();
        }
    }

    @Override
    public void wait(Object o, long timeout, int nanos) throws InterruptedException {
        try {
            enable();
            super.wait(o, timeout, nanos);
        }finally {
            disable();
        }
    }

    @Override
    public void beforeExecute(Thread t, JMVXJob r) {
        try {
            enable();
            super.beforeExecute(t, r);
        }finally {
            disable();
        }
    }

    @Override
    public void afterExecute(JMVXJob r, Throwable t) {
        try {
            enable();
            super.afterExecute(r, t);
        } finally {
            disable();
        }
    }

    @Override
    public void submit(JMVXJob task) {
        try {
            enable();
            super.submit(task);
        } finally {
            disable();
        }
    }

    @Override
    public void runnableFutureDone(JMVXQueueingFuture future) {
        try {
            enable();
            super.runnableFutureDone(future);
        } finally {
            disable();
        }
    }

    @Override
    public ExecutorService newFixedThreadPool(int nThreads, ThreadFactory threadFactory) {
        try {
            enable();
            return super.newFixedThreadPool(nThreads, threadFactory);
        } finally {
            disable();
        }
    }

    @Override
    public ExecutorService newCachedThreadPool(ThreadFactory threadFactory) {
        try {
            enable();
            return super.newCachedThreadPool(threadFactory);
        } finally {
            disable();
        }
    }

    @Override
    public void threadStart(Runnable r) {
        try {
            enable();
            super.threadStart(r);
        } finally {
            disable();
        }
    }

    @Override
    public void threadExit(Runnable r) {
        try {
            enable();
            super.threadExit(r);
        } finally {
            disable();
        }
    }

    @Override
    public void exitLogic() {
        try {
            enable();
            super.exitLogic();
        } finally {
            disable();
        }
    }

    @Override
    public void enter() {
        if(enabled) {
            super.enter();
        } else {
            //double enter to go to the passthrough
            super.enter();
            super.enter();
        }
    }

    @Override
    public void exit() {
        if(enabled) {
            super.exit();
        } else {
            //double exit to be symmetric with double enter
            super.exit();
            super.exit();
        }
    }
}
