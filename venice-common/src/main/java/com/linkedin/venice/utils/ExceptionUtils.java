package com.linkedin.venice.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Static utility functions to deal with exceptions.
 *
 * Copied from here:
 * https://github.com/voldemort/voldemort/blob/ea37ef67fa7724180608510c6d4237167b78dd63/src/java/voldemort/utils/ExceptionUtils.java
 */
public class ExceptionUtils {
  private static final Logger logger = LogManager.getLogger(ExceptionUtils.class);
  /**
   * Inspects a given {@link Throwable} as well as its nested causes, in order to look
   * for a specific set of exception classes. The function also detects if the throwable
   * to inspect is a subclass of one of the classes you look for, but not the other way
   * around (i.e.: if you're looking for the subclass but the throwableToInspect is the
   * parent class, then this function returns false).
   *
   * @return true if a the throwableToInspect corresponds to or is caused by any of the throwableClassesToLookFor
   */
  public static boolean recursiveClassEquals(Throwable throwableToInspect, Class... throwableClassesToLookFor) {
    if (null == throwableToInspect){
      return false;
    }
    for (Class clazz: throwableClassesToLookFor) {
      Class classToInspect = throwableToInspect.getClass();
      while (classToInspect != null) {
        if (classToInspect.equals(clazz)) {
          return true;
        }
        classToInspect = classToInspect.getSuperclass();
      }
    }
    Throwable cause = throwableToInspect.getCause();
    return recursiveClassEquals(cause, throwableClassesToLookFor);
  }

  /**
   * Inspects a given {@link Throwable} as well as its nested causes, in order to look
   * for a specific message.
   *
   * @return true if a the throwableToInspect contains the message parameter
   */
  public static boolean recursiveMessageContains(Throwable throwableToInspect, String message) {
    if (null == throwableToInspect){
      return false;
    }
    String throwableToInspectMessage = throwableToInspect.getMessage();
    if (null == throwableToInspectMessage) {
      return false;
    }
    if (throwableToInspectMessage.contains(message)) {
      return true;
    }
    Throwable cause = throwableToInspect.getCause();
    return recursiveMessageContains(cause, message);
  }

  /**
   * @return a String representation of the provided throwable's stacktrace.
   */
  public static String stackTraceToString(Throwable throwable) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    throwable.printStackTrace(pw);
    return sw.toString(); // stack trace as a string
  }

  public static String threadToThrowableToString(Thread thread) {
    if (null == thread) {
      return "null";
    }
    Throwable throwable = new Throwable();
    throwable.setStackTrace(thread.getStackTrace());
    return stackTraceToString(throwable);
  }

  public static void logClassLoaderContent(String packageName) {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    URL[] urls = ((URLClassLoader)cl).getURLs();
    Arrays.asList(urls).stream().filter(url -> url.getFile().contains(packageName)).forEach(logger::warn);
  }
}
