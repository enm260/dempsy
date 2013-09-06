package com.nokia.dempsy.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultDempsyExecutor implements DempsyExecutor
{
   private ThreadPoolExecutor executor = null;
   private AtomicLong numLimited = null;
   private long maxNumWaitingLimitedTasks = -1;
   private int threadPoolSize = -1;
   private static final int minNumThreads = 4;
   
   private double m = 1.25;
   private int additionalThreads = 2;
   
   public DefaultDempsyExecutor() { }
   
   /**
    * Create a DefaultDempsyExecutor with a fixed number of threads while setting the
    * maximum number of limited tasks.
    */
   public DefaultDempsyExecutor(int threadPoolSize, int maxNumWaitingLimitedTasks) 
   {
      this.threadPoolSize = threadPoolSize;
      this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
   }
   
   /**
    * <p>Prior to calling start you can set the cores factor and additional
    * cores. Ultimately the number of threads in the pool will be given by:</p> 
    * 
    * <p>num threads = m * num cores + b</p>
    * 
    * <p>Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads</p>
    */
   public void setCoresFactor(double m){ this.m = m; }
   
   /**
    * <p>Prior to calling start you can set the cores factor and additional
    * cores. Ultimately the number of threads in the pool will be given by:</p> 
    * 
    * <p>num threads = m * num cores + b</p>
    * 
    * <p>Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads</p>
    */
   public void setAdditionalThreads(int additionalThreads){ this.additionalThreads = additionalThreads; }
   
   @Override
   public void start()
   {
      if (threadPoolSize == -1)
      {
         // figure out the number of cores.
         int cores = Runtime.getRuntime().availableProcessors();
         int cpuBasedThreadCount = (int)Math.ceil((double)cores * m) + additionalThreads; // why? I don't know. If you don't like it 
                                                                                          //   then use the other constructor
         threadPoolSize = Math.max(cpuBasedThreadCount, minNumThreads);
      }
      executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize);
      numLimited = new AtomicLong(0);
      
      if (maxNumWaitingLimitedTasks < 0)
         maxNumWaitingLimitedTasks = 20 * threadPoolSize;
   }
   
   public int getMaxNumberOfQueuedLimitedTasks() { return (int)maxNumWaitingLimitedTasks; }
   
   public void setMaxNumberOfQueuedLimitedTasks(int maxNumWaitingLimitedTasks) { this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks; }
   
   @Override
   public int getNumThreads() { return threadPoolSize; }
   
   @Override
   public void shutdown()
   {
      if (executor != null)
         executor.shutdown();
   }

   @Override
   public int getNumberPending()
   {
      return executor.getQueue().size();
   }
   
   @Override
   public int getNumberLimitedPending()
   {
      return numLimited.intValue();
   }

   
   public boolean isRunning() { return (executor != null) &&
         !(executor.isShutdown() || executor.isTerminated()); }
   
   @Override
   public <V> Future<V> submit(Callable<V> r) { return executor.submit(r); }

   @Override
   public <V> void submitLimited(final Rejectable<V> r)
   {
      Callable<V> task = new Callable<V>()
      {
         Rejectable<V> o = r;

         @Override
         public V call() throws Exception
         {
            long num = numLimited.decrementAndGet();
            if (num <= maxNumWaitingLimitedTasks)
               return o.call();
            o.rejected();
            return null;
         }
      };
      
      long nextCount = numLimited.incrementAndGet();
      
      // if we just pushed the count beyond twice the max
      //  then we should just drop the incoming message
      if (nextCount > (maxNumWaitingLimitedTasks << 1))
         r.rejected();
      else
         executor.submit(task);
   }
}
