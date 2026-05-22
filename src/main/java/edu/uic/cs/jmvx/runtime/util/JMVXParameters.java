package edu.uic.cs.jmvx.runtime.util;

import edu.uic.cs.jmvx.runtime.JMVXRuntime;
import edu.uic.cs.jmvx.vectorclock.HybridVectorClock;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Class that holds a bunch of final fields that encode the settings of supplied system parameters
 * JMVX is loaded as part of the bootstrap classes, so it can be loaded BEFORE system variables are parsed
 * As a result, JMVX would set the value of a parameter to a DEFAULT, rather than the one supplied.
 * Idea behind this class is to set the parameters later, after JMVX has been initialized
 *
 * NOTE: this class reads the properties when an instance is created!
 * Don't make the instance until you are ready and able to read System properties or file data!
 */
public class JMVXParameters {
    //Runtime (all)
    public final String role;
    public final String logfile;
    public final String vectorclock;
    public final Boolean reuseVCSlots;
    public final boolean checksumCommunicationEnabled;
    public final boolean readsFromDisk;
    public final boolean avoidObjectSerialization;
    public final boolean useBufferBackedStreams;

    //Leader roles
    public final boolean validateWrites;
    public final boolean fastExit;
    public final boolean computePartialOrders;

    //RR roles
    public final String recordingDir;

    //Follower Roles
    public final String divergenceHandlerClassName;
    public final int junkPort;

    //FollowerRecorderImpl
    public final boolean delayedWriteRecording;
    public final boolean markElidedEvents;

    //Circular Buffer
    public final boolean circularBufferCommunicationEnabled;
    public final long mappedMemorySize;
    public final String mappedMemoryLocation;
    public final boolean deleteMmapOnDisk;

    //Debug/Statistics
    public final boolean consoleLogsEnabled;
    public final boolean coreDumpAtEnd;


    //Instrumenter
    public final boolean sync;
    public final boolean logNatives;

    public JMVXParameters() {

        // If jmvxProperties is supplied, read additional properties from a file
        String propertyFilename = System.getProperty("jmvx.properties");
        // inherit defaults from the system
        Properties properties = new Properties(System.getProperties());
        if(propertyFilename != null){
            try(FileInputStream fileIn = new FileInputStream(propertyFilename)){
                properties.load(fileIn);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Properties file not found", e);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse properties file", e);
            }
        }

        // Runtime Parameters
        role = properties.getProperty(JMVXRuntime.ROLE_PROP_NAME);
        logfile = properties.getProperty(JMVXRuntime.LOGFILE_PROP_NAME, role + ".log");
        vectorclock = properties.getProperty("vectorclock", HybridVectorClock.class.getSimpleName());
        reuseVCSlots = Boolean.parseBoolean(properties.getProperty("reuseVCSlots", "true"));
        checksumCommunicationEnabled = Boolean.parseBoolean(properties.getProperty("checksumCommunicationEnabled", "0"));
        readsFromDisk = Boolean.parseBoolean(properties.getProperty("readsFromDisk", "0"));
        avoidObjectSerialization = Boolean.parseBoolean(properties.getProperty("avoidObjectSerialization", "true"));
        useBufferBackedStreams = Boolean.parseBoolean(properties.getProperty("useBufferBackedStreams", "true"));

        //Leader Roles
        validateWrites = Boolean.parseBoolean(properties.getProperty("validateWrites", "false"));
        fastExit = properties.getProperty("fastExit", "true").equals("true");
        computePartialOrders = Boolean.parseBoolean(properties.getProperty("computePartialOrders", "true"));

        //RR Roles
        recordingDir = properties.getProperty("recordingDir", "rec");

        //Follower Roles
        divergenceHandlerClassName = properties.getProperty("divergenceHandler", "edu.uic.cs.jmvx.runtime.DacapoDivergenceHandler");
        junkPort = Integer.parseInt(properties.getProperty("junkPort", "9999"));

        //FollowerReplayerImpl
        delayedWriteRecording = Boolean.parseBoolean(properties.getProperty("delayedWriteRecording", "true"));
        markElidedEvents = Boolean.parseBoolean(properties.getProperty("markElidedEvents", "true"));

        //CircularBuffer
        circularBufferCommunicationEnabled = Boolean.parseBoolean(properties.getProperty("circularBufferCommunicationEnabled", "true"));
        mappedMemorySize = Long.parseLong(properties.getProperty("mappedMemorySize", "819200000"));
        mappedMemoryLocation = properties.getProperty("mappedMemoryLocation", "/dev/shm/");
        deleteMmapOnDisk = Boolean.parseBoolean(properties.getProperty("deleteMmapOnDisk", "true"));

        //Debug/Stats
        consoleLogsEnabled = Boolean.parseBoolean(properties.getProperty("consoleLogsEnabled", "true"));
        coreDumpAtEnd = Boolean.parseBoolean(properties.getProperty("coreDumpAtEnd", "false"));

        // Instrumenter Parameters
        sync = Boolean.parseBoolean(properties.getProperty("sync", "true"));
        logNatives = Boolean.parseBoolean(properties.getProperty("logNatives", "false"));
    }
}
