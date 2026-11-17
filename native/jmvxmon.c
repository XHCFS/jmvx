#include <jni.h>
JNIEXPORT void JNICALL Java_edu_uic_cs_jmvx_runtime_JMVXMonitor_enter(JNIEnv *e, jclass c, jobject o) { (*e)->MonitorEnter(e, o); }
JNIEXPORT void JNICALL Java_edu_uic_cs_jmvx_runtime_JMVXMonitor_exit(JNIEnv *e, jclass c, jobject o) { (*e)->MonitorExit(e, o); }
