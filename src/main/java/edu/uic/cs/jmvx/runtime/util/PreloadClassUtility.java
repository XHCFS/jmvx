package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.runtime.JMVXClassLoader;
import edu.uic.cs.jmvx.runtime.JMVXRuntime;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

public class PreloadClassUtility {

    public static final boolean ENABLED = true;

    private static List<String> classesToForceLoad = Arrays.asList(
            //batik
            "java.nio.DirectByteBuffer$Deallocator",
            Formatter.class.getName(),
            //FileChannel.class.getName() <- abstract
            "sun.nio.ch.FileChannelImpl",
            "sun.nio.ch.NativeThreadSet",
            "sun.nio.ch.FileDispatcherImpl", //abstract parent class is where the exception was
            //fop
            "sun.util.locale.provider.SPILocaleProviderAdapter", //triggers load of abstract parent classes
            "sun.util.locale.provider.LocaleProviderAdapter$1",
            "sun.util.locale.provider.LocaleDataMetaInfo",
            "sun.util.locale.LanguageTag",
            "sun.util.locale.provider.LocaleResources",
            "sun.util.resources.LocaleData$LocaleDataResourceBundleControl",
            "sun.util.locale.provider.LocaleServiceProviderPool",
            //pmd
            "sun.nio.cs.ISO_8859_1$Decoder"
            //jython

    );

    private static void preloadWithExecution(){
        //write java code to execute a function in a class that needs to be preloaded
        //similar to what we do for Formatter in JMVXRuntime's initialization
        //really that should be moved here.
        //java/nio/channels/spi/SelectorProvider, System.inheritedChanell()
        try {
            System.inheritedChannel();
        } catch (IOException e) {
            //e.printStackTrace();
        }
    }

    public static void preloadClasses(){
        if(ENABLED){
            final JMVXClassLoader loader = (JMVXClassLoader) ClassLoader.getSystemClassLoader();
            classesToForceLoad.forEach((className) -> {
                try {
                    Class<?> clazz = loader.$JMVX$loadClass(className);
                    //really just want to trigger the clinit
                    JMVXRuntime.unsafe.allocateInstance(clazz);
                } catch (ClassNotFoundException | InstantiationException e) {
                    e.printStackTrace();
                }
            });
            preloadWithExecution();
        }
    }
}
