package com.nokia.dempsy.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lmax.disruptor.util.PaddedLong;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.executor.DempsyExecutor.Rejectable;

public class TestLockFreeExecutor
{
   private static long baseTimeoutMillis = 20000;

   LockFreeExecutor executor = null;
   static AtomicLong numRejected = new AtomicLong(0);
   static AtomicInteger numLimitedPending = new AtomicInteger(0);
   static private final int threadPoolSize = 8;
   
   @Before
   public void initExecutor()
   {
      numRejected.set(0);
      numLimitedPending.set(0);
      executor = new LockFreeExecutor(threadPoolSize,20 * threadPoolSize);
   }
   
   @After
   public void shutdownExecutor()
   {
      if (executor != null)
         executor.shutdown();
      executor = null;
   }
   
   final PaddedLong result = new PaddedLong(0);
   
   public class Task implements Rejectable
   {
      final AtomicBoolean latch;
      final AtomicLong count;
      
      Task(AtomicBoolean latch, AtomicLong count) { this.latch = latch; this.count = count; }
      
      @Override
      public void run()
      {
         if (latch != null)
         {
            synchronized(latch)
            {
               while (!latch.get())
                  try { latch.wait(); } catch (InterruptedException ie) {}
            }
         }
         
         if (count != null) count.incrementAndGet();
         
         result.set(result.get() + 1L);
      }

      @Override
      public void rejected()
      {
         // we don't need to do anything as the Future will have "null" if this gets called
         numRejected.incrementAndGet();
      }
   }
   
   @Test
   public void testSimple() throws Throwable
   {
      result.set(0);
      executor.start();
      
      Task task = new Task(null,null);
      executor.submit(task);
      
      assertTrue(TestUtils.poll(baseTimeoutMillis, result, new TestUtils.Condition<PaddedLong>()
      {
         @Override public boolean conditionMet(PaddedLong o) { return 1L == result.get(); }
      }));
      
      submitLimited(executor,task);
      assertTrue(TestUtils.poll(baseTimeoutMillis, result, new TestUtils.Condition<PaddedLong>()
      {
         @Override public boolean conditionMet(PaddedLong o) { return 2L == result.get(); }
      }));
   }
   
   @Test
   public void testLimited() throws Throwable
   {
      executor.start();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         int numThreads = executor.getNumThreads();

         Task task = new Task(latch,null);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            submitLimited(executor,task);

         // submit twice as many.
         for (int i = 0; i < numThreads; i++)
            submitLimited(executor,task);

         // none should be rejected yet.
         assertEquals(0,numRejected.get());

         // now submit until they start getting rejected.
         TestUtils.poll(baseTimeoutMillis, task, new TestUtils.Condition<Task>() 
         {  @Override 
            public boolean conditionMet(Task task) throws Throwable
            {
               submitLimited(executor,task); submitLimited(executor,task); submitLimited(executor,task);
               synchronized(task.latch) { task.latch.set(true); task.latch.notify(); }
               return numRejected.get() > 0;
            }
         });
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   
   @Test
   public void testExactLimited() throws Throwable
   {
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final int numThreads = executor.getNumThreads();
         final AtomicLong execCount = new AtomicLong(0);

         Task blockingTask = new Task(latch,execCount);
         Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            submitLimited(executor,blockingTask);

         // none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 0;
            }
         }));
         
         // now submit one more
         submitLimited(executor,nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,numLimitedPending.get());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() { } });

         // now the pending tasks should go to two.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,numLimitedPending.get());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            submitLimited(executor,nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return numLimitedPending.get() == maxQueued;
            }
         }));

         // The queue math is NOT conservative in the LockFreeExecutor case. See the other test
         // submitLimited(executor,nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         // because the LockFreeExecutor is not conservative, this calculation doesn't
         // add the final '1' like the DefaultDempsyExecutor test does.
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1);

         // wait until they all execute
         TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         });
         assertEquals(totalCountingExecs,execCount.intValue());
         
         // we should have discarded none.
         // now the number limited pending should be the maxQueued
         Thread.sleep(10);
         assertEquals(0,numRejected.intValue());
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   
   @Test
   public void testExactLimitedOverflow() throws Throwable
   {
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final AtomicLong execCount = new AtomicLong(0);
         int numThreads = executor.getNumThreads();

         Task blockingTask = new Task(latch,execCount);
         Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            submitLimited(executor,blockingTask);

         // none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 0;
            }
         }));

         // now submit one more
         submitLimited(executor,nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,numLimitedPending.get());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() { } });

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,numLimitedPending.get());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            submitLimited(executor,nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) throws Throwable
            {   
               return numLimitedPending.get() == maxQueued;
            }
         }));

         // The queue math is NOT conservative ... see above
         //submitLimited(executor,nonblockingTask);

         // now if I add one more I should see exactly one discard.
         submitLimited(executor,nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
         
         // because the LockFreeExecutor is not conservative, this calculation doesn't
         // add the final '1' like the DefaultDempsyExecutor test does.
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1);
         
         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));

         Thread.sleep(10);
         assertEquals(1,numRejected.intValue());

         // and eventually none should be queued
         TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<LockFreeExecutor>() {
            @Override public boolean conditionMet(LockFreeExecutor executor) { return executor.getNumberPending() == 0; }
         });
         assertEquals(0,executor.getNumberPending());
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   
   static class LimitedTask implements Rejectable
   {
      Task task;
      LimitedTask(Task task) { this.task = task; }
      
      @Override public void run() { numLimitedPending.decrementAndGet(); task.run(); }
      
      @Override public void rejected() { numLimitedPending.decrementAndGet(); task.rejected(); }
   }
   
   public final static void submitLimited(DempsyExecutor executor, Task task)
   {
      numLimitedPending.incrementAndGet();
      executor.processMessage(new LimitedTask(task));
   }
}
