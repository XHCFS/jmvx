package edu.uic.cs.jmvx.bytecode;

import edu.uic.cs.jmvx.runtime.util.UIDedObject;
import org.objectweb.asm.*;

import java.lang.ref.Reference;
import java.util.Optional;

import static edu.uic.cs.jmvx.bytecode.BytecodeUtils.addInterface;

public class InjectUIDedObject extends ClassVisitor implements Opcodes {

    public boolean activated = false;

    private final String UIDED_OBJECT_INTERNAL_NAME = Type.getInternalName(UIDedObject.class);
    private final String OBJECT_INTERNAL_NAME = Type.getInternalName(Object.class);
    Optional<ClassLoader> opttionalLoader;

    public InjectUIDedObject(Optional<ClassLoader> opttionalLoader, ClassVisitor classVisitor) {
        super(ASM7, classVisitor);
        this.opttionalLoader = opttionalLoader;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        /* criteria:
        * not an interface
        * super is Object
        * not in java.lang package (or any subpackages). Prevents lots of segfaults
         */
        activated = ((ACC_INTERFACE & access) == 0) &&
                superName != null && superName.equals(OBJECT_INTERNAL_NAME) &&
                !name.startsWith("java/lang"); //has issues with these classes
                /*!opttionalLoader.map((loader) -> {
                    try {
                        return Reference.class.isAssignableFrom(
                                Class.forName(Type.getType(String.format("L%s;", name)).getClassName(),
                                        false, loader));
                    } catch (ClassNotFoundException | NoClassDefFoundError | IllegalAccessError e) {
                        return false;
                    }}).orElse(false);*/
        superName = activated ? UIDED_OBJECT_INTERNAL_NAME : superName;
        super.visit(version, access, name, signature, superName, interfaces);
    }

    private final String initMethod = "<init>";

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        return new MethodVisitor(ASM7, super.visitMethod(access, name, descriptor, signature, exceptions)) {
            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
                if(activated) {
                    //need to change owner of calls to supers and such
                    if(name.equals(initMethod) && owner.equals(OBJECT_INTERNAL_NAME)){
                        //in constructor calling super, we need to change the owner
                        owner = UIDED_OBJECT_INTERNAL_NAME;
                    } // else { /* nop */ }
                }
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }

            @Override
            public void visitTypeInsn(int opcode, String type) {
                //new UIDedObject instead of new Object
                if(activated && opcode == NEW && type.equals(OBJECT_INTERNAL_NAME)){
                    type = UIDED_OBJECT_INTERNAL_NAME;
                }
                super.visitTypeInsn(opcode, type);
            }
        };
    }
}
