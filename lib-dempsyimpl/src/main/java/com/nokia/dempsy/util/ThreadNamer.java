package com.nokia.dempsy.util;

import java.util.concurrent.Executor;

import org.slf4j.Logger;

import com.nokia.dempsy.executor.DempsyExecutor;

/**
 * Names threads in a thread pool. Only works for fixed size pools.
 */
public class ThreadNamer implements Runnable
{
   private final Logger logger;
   private final int threadCount;
   private final String baseName;
   private long sequence = 0;
   
   // milliseconds per thread
   private static final long namingTimeoutLimitFactorMillis = 100;
   
   public ThreadNamer(String baseName, int threadCount, Executor executor, Logger logger)
   {
      this.logger = logger;
      this.threadCount = threadCount;
      this.baseName = baseName;
      for (int i = 0; i < threadCount; i++)
         executor.execute(this);
   }
   
   @Override
   public synchronized void run()
   {
      String threadName = baseName + "-" + sequence++;
      Thread.currentThread().setName(threadName);
      
      if (sequence >= threadCount)
         notifyAll();
      else
         // block until sequence gets to threadCount
         while (sequence < threadCount)
         {
            try { wait(namingTimeoutLimitFactorMillis * threadCount); } catch (InterruptedException ie) {}
         }
      
      if (sequence < threadCount)
         logger.error("Failed to set all of the name's for the " + DempsyExecutor.class.getSimpleName() + 
               ". This is either a bug in the Dempsy code OR the JVM is under tremendous load.");
   }
}
