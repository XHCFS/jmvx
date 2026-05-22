package edu.uic.cs.jmvx.bytecode;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ExecutorsClassVisitor extends ClassVisitor implements Opcodes {

    private static Type EXECUTORS = Type.getType(Executors.class);
    private static Type JMVX_RUNTIME = Type.getType(JMVXRuntime.class);

    public ExecutorsClassVisitor(ClassVisitor classVisitor) {
        super(ASM7, classVisitor);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {

        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
        return new MethodVisitor(ASM7, mv) {
            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                if (opcode == INVOKESTATIC && owner.equals(EXECUTORS.getInternalName())
                        && (name.equals("newCachedThreadPool") || name.equals("newFixedThreadPool"))) {
                    owner = JMVX_RUNTIME.getInternalName();
                }
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        };
    }
}
