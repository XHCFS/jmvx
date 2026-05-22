package edu.uic.cs.jmvx.runtime.client;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.runtime.strategy.JMVXStrategy;
import edu.uic.cs.jmvx.runtime.strategy.SingleLeaderWithoutCoordinator;

import java.util.Optional;

public class RecorderController {

    public static final String roleName = JMVXRuntime.parameters.role;
    public static final Optional<Class> role;

    static{
        Optional<Class> temp;
        try {
            temp = Optional.of(Class.forName(JMVXStrategy.class.getPackage().getName() + "." + roleName));
        } catch (ClassNotFoundException _) {
            temp = Optional.empty();
        }
        role = temp; //can only assign once b/c final
    }

    public static boolean isRecorder(){
        return role.map((c) -> c.getSimpleName().startsWith("Recorder"))
                .orElse(Boolean.FALSE);
        }

    public static boolean isVanillaOrSignleLeader(){
        return role.map((c) -> c.equals(SingleLeaderWithoutCoordinator.class))
                .orElse(Boolean.TRUE);
    }

    public static void skip(Runnable r){
        //No need to make a new role
        if(isVanillaOrSignleLeader()) {
            r.run();
        }else if(isRecorder()) { //have to save/restore role
            /*Change strategy to one which doesn't record and then execute
              before finally restoring the strategy */
            JMVXStrategy temp = JMVXRuntime.getStrategy();
            try {
                JMVXRuntime.setStrategy(new SingleLeaderWithoutCoordinator());
                r.run();
            } finally {
                JMVXRuntime.setStrategy(temp);
            }
        } // else skip!
    }

    public static void doNotRecord(Runnable r){
        //No need to make a new role
        if(isVanillaOrSignleLeader()) {
            r.run();
        }else{ //have to save/restore role
            /*no check for role as all roles need to run the code
              but we do not want to log events
              Change strategy to one which doesn't record and then execute
              before finally restoring the strategy*/
            JMVXStrategy temp = JMVXRuntime.getStrategy();
            try {
                JMVXRuntime.setStrategy(new SingleLeaderWithoutCoordinator());
                r.run();
            } finally {
                JMVXRuntime.setStrategy(temp);
            }
        }
    }
}
