package edu.uic.cs.jmvx.bytecode;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Used to make methods that aren't synchronized synchronized.
 * Used for two purposes: fix (some instances of double check locking and make caches trackable
 *
 * NOTE: this does not handle the double check locking in Sunflow.
 * In Sunflow, the double check locking is fine is it happens out of order because
 * Geometry.intersect (the method that uses double check) does not cause loggable events
 * That issue is handled via the deny list in SynchronizedClassVisitor
 */
public class MakeSynchronizedClassVisitor extends ClassVisitor implements Opcodes {
    private String methodName;
    private Boolean inTargetClass = false;

    private static final HashMap<String, String> toFix = new HashMap<>();

    static {
        //lazy initializer of static variables.
        toFix.put("org/apache/xml/serializer/OutputPropertiesFactory", "getDefaultMethodProperties");
        //race to put data in the cache (in Xalan). First thread has different operations than all others after
        toFix.put("org/apache/xml/serializer/CharInfo", "getCharInfo");
        //ensure tasks finish in the same order as leader/recording
        //TODO put a switch here, so MVX mode can still use this
        //AH we do need to merge the instrumentation so MVX can still work....
        //toFix.put("java/util/concurrent/ExecutorCompletionService$QueueingFuture", "done");

        //CAS to set seedUniquifier field, used in computation of future seeds
        //toFix.put(Type.getInternalName(Random.class), "seedUniquifier");
        //AtomicInteger use and modification when computing seed. Order of initialization effects future seeds
        //toFix.put((Type.getInternalName(ThreadLocalRandom.class)), "localInit");
    }

    public MakeSynchronizedClassVisitor(org.objectweb.asm.ClassVisitor classVisitor) {
        super(ASM7, classVisitor);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        methodName = toFix.get(name);
        inTargetClass = methodName != null;
        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        //should check descriptor, but I don't think any other variant of this method exists
        if(inTargetClass && name.equals(methodName)){
            access += ACC_SYNCHRONIZED; //acc sync flag
        }
        return super.visitMethod(access, name, descriptor, signature, exceptions);
    }
}
