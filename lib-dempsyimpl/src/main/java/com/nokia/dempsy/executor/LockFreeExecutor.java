package com.nokia.dempsy.executor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.util.PaddedLong;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.util.RingBufferConsumerControl;
import com.nokia.dempsy.util.RingBufferControl;
import com.nokia.dempsy.util.RingBufferControlWorkerPool;
import com.nokia.dempsy.util.ThreadNamer;

/**
 * The LockFreeExecutor can work but ONLY if the publisher of processMessage
 * calls is always called from the same thread and the caller of submit is
 * always the same publishing thread (though they don't have to be the same 
 * thread).
 */
public class LockFreeExecutor implements DempsyExecutor
{
   private static final Logger logger = LoggerFactory.getLogger(DefaultDempsyExecutor.class);
   private static final AtomicLong executorCountSequence = new AtomicLong(0);
   
   private final long executorCount;
   private final AtomicReference<RingBufferControlWorkerPool> curRingBufferControlWorkerPoolAtomicRef = new AtomicReference<RingBufferControlWorkerPool>(null);
   private final AtomicReference<IndexedRunnable[]> curBufferAtomicRef = new AtomicReference<IndexedRunnable[]>(null);
   private RingBufferControlWorkerPool curRingBufferControlWorkerPoolCache;
   private IndexedRunnable[] curBufferCached;
   
   // These are used to double the RingBuffer size dynamically
   private final AtomicReference<RingBufferControlWorkerPool> nextRingBufferControlWorkerPool = new AtomicReference<RingBufferControlWorkerPool>(null);
   private final AtomicLong nextCountDown = new AtomicLong(0);
   
   private boolean isRunning = false;

   private ThreadPoolExecutor executor;
   private final int threadPoolSize;
   
   private final PaddedLong numLimitedSubmitted = new PaddedLong(0);
   private final long maxNumWaitingLimitedTasks;
   private final PaddedLong[] sequenceLimitedExecuted;
   
   public LockFreeExecutor(int numThreads, int maxNumWaitingLimitedTasks) throws IllegalArgumentException
   {
      this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
      // we need to find the next higher power of 2.
      long sizePowerOfTwo = 1;
      while (sizePowerOfTwo <= maxNumWaitingLimitedTasks) 
         sizePowerOfTwo <<= 1;
      
      this.curRingBufferControlWorkerPoolAtomicRef.set(new RingBufferControlWorkerPool(numThreads,(int)sizePowerOfTwo));
      this.curRingBufferControlWorkerPoolCache = curRingBufferControlWorkerPoolAtomicRef.get();
      this.curBufferAtomicRef.set(new IndexedRunnable[(int)sizePowerOfTwo]);
      this.curBufferCached = curBufferAtomicRef.get();
      this.executorCount = executorCountSequence.getAndIncrement();
      this.threadPoolSize = numThreads;
      this.sequenceLimitedExecuted = new PaddedLong[threadPoolSize];
      for (int i = 0; i < threadPoolSize; i++)
         this.sequenceLimitedExecuted[i] = new PaddedLong(0);
   }
   
   private class IndexedRunnable implements Runnable
   {
      private final Runnable proxied;
      private final long sequence;
      private final boolean rejectable;

      public IndexedRunnable(Rejectable p, long sequence)
      {
         this.proxied = p;
         this.sequence = sequence;
         rejectable = true;
      }
      
      public IndexedRunnable(Runnable p)
      {
         this.proxied = p;
         this.sequence = -1L;
         rejectable = false;
      }
      
      @Override public void run()
      {
         final long num = numLimitedSubmitted.get() - sequence;
         if (num <= maxNumWaitingLimitedTasks)
            proxied.run();
         else if (rejectable)
            ((Rejectable)proxied).rejected();
      }

      public void setSequence(int index)
      {
         if (rejectable)
            sequenceLimitedExecuted[index].set(sequence); 
      }
   }
   
   private void doSubmit(final IndexedRunnable r)
   {
      long nextSequence = curRingBufferControlWorkerPoolCache.tryNext();
      if (RingBufferControl.UNAVAILABLE == nextSequence)
      {
         //====================================================================
         // grow the current RingBuffer
         final int nextBufferSize = curRingBufferControlWorkerPoolCache.getBufferSize() << 1;
         curBufferAtomicRef.set(new IndexedRunnable[nextBufferSize]);
         curBufferCached = curBufferAtomicRef.get();
         nextCountDown.set(threadPoolSize);
         nextRingBufferControlWorkerPool.set(new RingBufferControlWorkerPool(threadPoolSize, nextBufferSize));
         // post the stop to signal the transition
         curRingBufferControlWorkerPoolCache.publishStop();
         curRingBufferControlWorkerPoolCache = nextRingBufferControlWorkerPool.get();
         //====================================================================

         nextSequence = curRingBufferControlWorkerPoolCache.next();
      }

      curBufferCached[curRingBufferControlWorkerPoolCache.index(nextSequence)] = r;
      curRingBufferControlWorkerPoolCache.publish(nextSequence);
   }

   /**
    * This implementation requires that the submit and processMessage is always called 
    * from the same thread.
    */
   @Override
   public void submit(final Runnable r) { doSubmit(new IndexedRunnable(r)); }
   
   @Override
   public void processMessage(final Rejectable r)
   {
      final long limitedSequence = numLimitedSubmitted.get();
      numLimitedSubmitted.set(limitedSequence + 1L);
      
      doSubmit(new IndexedRunnable(r,limitedSequence));
   }

   @Override
   public int getNumberPending()
   {
      return (int)curRingBufferControlWorkerPoolAtomicRef.get().getNumEntries();
   }
   
//   @Override
//   public int getNumberLimitedPending()
//   {
//      long maximum = sequenceLimitedExecuted[0].get();
//      for (int i = 1; i < threadPoolSize; i++)
//         maximum = Math.max(maximum, sequenceLimitedExecuted[i].get());
//      return (int)(numLimitedSubmitted.get() - maximum - 1);
//   }
   
   public int getMaxNumberOfQueuedLimitedTasks() { return (int)maxNumWaitingLimitedTasks; }
   
   private void cleanupTransition()
   {
      nextRingBufferControlWorkerPool.set(null);
      nextCountDown.set(0);
   }
   
   @Override
   public void start()
   {
      if (isRunning)
         return;
      
      executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize);
      String baseName = "DempsyExc-" + executorCount;
      new ThreadNamer(baseName, threadPoolSize, executor,logger);
      final CountDownLatch allStarted = new CountDownLatch(threadPoolSize);
      
      for (int i = 0; i < threadPoolSize; i++)
      {
         final int tindex = i;
         final RingBufferConsumerControl rbc = curRingBufferControlWorkerPoolAtomicRef.get().get(tindex);
         
         executor.submit(new Runnable()
         {
            final int index = tindex;
            RingBufferConsumerControl consumer = rbc;
            IndexedRunnable[] buffer = curBufferAtomicRef.get();
            
            @Override public void run()
            {
               allStarted.countDown();
               
               while (true)
               {
                  try
                  {
                     final long availableTo = consumer.availableTo();
                     if (availableTo == RingBufferConsumerControl.ACQUIRE_STOP_REQUEST)
                     {
                        // we need to see if this means we're switching to a new ringbuffer.
                        final RingBufferControlWorkerPool rbc = nextRingBufferControlWorkerPool.get();
                        if (rbc == null) // if there's no next RingBuffer then we just shut down.
                           break;
                        
                        // otherwise we need to prepare the new data.
                        buffer = curBufferAtomicRef.get();
                        consumer = rbc.get(index);
                        final long countDown = nextCountDown.decrementAndGet();
                        if (countDown <= 0)
                           cleanupTransition();
                     }
                     
                     IndexedRunnable cur = buffer[consumer.index(availableTo)];
                     try { cur.setSequence(index); cur.run(); }
                     catch (Throwable th) { logger.error("Exception in callable. " + SafeString.objectDescription(cur),th); }
                     consumer.notifyProcessed();
                  }
                  catch(Throwable th)
                  {
                     logger.error("Unknown exception in " + LockFreeExecutor.class.getSimpleName(),th);
                  }
               }
            }
         });
      }
      for (boolean done = false; !done;)
      {
         try { allStarted.await(); done = true; } catch(InterruptedException e) {}
      }

      isRunning = true;
   }

   /**
    * Shutdown needs to be called from the processMessage thread.
    */
   @Override
   public void shutdown()
   {
      if (!isRunning)
         return;
      cleanupTransition();
      curRingBufferControlWorkerPoolAtomicRef.get().publishStop();
      isRunning = false;
   }

   @Override
   public int getNumThreads() { return threadPoolSize; }

}
