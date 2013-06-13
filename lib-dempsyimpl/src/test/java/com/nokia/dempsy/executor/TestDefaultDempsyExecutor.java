package com.nokia.dempsy.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lmax.disruptor.util.PaddedLong;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.executor.DempsyExecutor.Rejectable;

public class TestDefaultDempsyExecutor
{
   private static long baseTimeoutMillis = 20000;

   DefaultDempsyExecutor executor = null;
   static AtomicLong numRejected = new AtomicLong(0);
   
   @Before
   public void initExecutor()
   {
      numRejected.set(0);
      executor = new DefaultDempsyExecutor();
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
      
      executor.processMessage(task);
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
            executor.processMessage(task);

         // submit twice as many.
         for (int i = 0; i < numThreads; i++)
            executor.processMessage(task);

         // none should be rejected yet.
         assertEquals(0,numRejected.get());

         // now submit until they start getting rejected.
         TestUtils.poll(baseTimeoutMillis, task, new TestUtils.Condition<Task>() 
         {  @Override 
            public boolean conditionMet(Task task) throws Throwable
            {
               executor.processMessage(task); executor.processMessage(task);executor.processMessage(task);
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
            executor.processMessage(blockingTask);

         // none should be queued
         Thread.sleep(10);
         assertEquals(0,executor.getNumberPending());
         
         // now submit one more
         executor.processMessage(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() { } });

         // now the pending tasks should go to two.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.processMessage(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.processMessage(nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1;

         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));
         
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
   public void testBlocking() throws Throwable
   {
      executor.setBlocking(true);
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final int numThreads = executor.getNumThreads();
         final AtomicLong execCount = new AtomicLong(0);

         final Task blockingTask = new Task(latch,execCount);
         final Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.processMessage(blockingTask);

         // none should be queued
         Thread.sleep(10);
         assertEquals(0,executor.getNumberPending());
         
         // now submit one more
         executor.processMessage(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() {  } });

         // now the pending tasks should go to two.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.processMessage(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.processMessage(nonblockingTask);

         // submit another that will block so we need to do it in another thread
         final AtomicLong pos = new AtomicLong(0);
         Thread thread = new Thread(new Runnable() 
         {
            @Override public void run() 
            {
               pos.incrementAndGet();
               executor.processMessage(nonblockingTask);
               pos.incrementAndGet();
            }
         });
         thread.start();
         
         assertTrue(TestUtils.poll(baseTimeoutMillis, pos, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return 1 == c.intValue(); }
         }));
         
         Thread.sleep(10);
         assertEquals(1,pos.intValue());

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         // includes the task that's blocking on the submit
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1 + 1;

         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));
         
         // The submitted blocked task should have made it in.
         assertTrue(TestUtils.poll(baseTimeoutMillis, pos, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return 2 == c.intValue(); }
         }));

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
   public void testStopDuringBlock() throws Throwable
   {
      executor.setBlocking(true);
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         final int numThreads = executor.getNumThreads();
         final AtomicLong execCount = new AtomicLong(0);

         final Task blockingTask = new Task(latch,execCount);
         final Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.processMessage(blockingTask);

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 0; i < maxQueued; i++)
            executor.processMessage(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.processMessage(nonblockingTask);

         // submit another that will block so we need to do it in another thread
         final AtomicLong pos = new AtomicLong(0);
         Thread thread = new Thread(new Runnable() 
         {
            @Override public void run() 
            {
               pos.incrementAndGet();
               try { executor.processMessage(nonblockingTask); }
               catch (RuntimeException re) { /* we expect a runtime exception since the executor will be shut down */ }
               finally { pos.incrementAndGet(); }
            }
         });
         thread.start();
         
         assertTrue(TestUtils.poll(baseTimeoutMillis, pos, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return 1 == c.intValue(); }
         }));
         
         Thread.sleep(10);
         assertEquals(1,pos.intValue());
         
         executor.shutdown();

         // The submitted blocked task should have made it in.
         assertTrue(TestUtils.poll(baseTimeoutMillis, pos, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return 2 == c.intValue(); }
         }));

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         // doesn't includes the task that's blocking on the submit since it was rejected
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1;

         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));
         
         // we should have rejected the one that was blocking when the executor was stopped.
         assertTrue(TestUtils.poll(baseTimeoutMillis, numRejected, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return 1 == c.intValue(); }
         }));

         // and it should stay that way.
         Thread.sleep(10);
         assertEquals(1,numRejected.intValue());
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
   
   @Test
   public void testExactLimitedUnlimited() throws Throwable
   {
      executor.setUnlimited(true);
      executor.start();
      
      final int maxQueued = executor.getMaxNumberOfQueuedLimitedTasks();
      
      final AtomicBoolean latch = new AtomicBoolean(false);
      try
      {
         int numThreads = executor.getNumThreads();
         final AtomicLong execCount = new AtomicLong(0);

         Task blockingTask = new Task(latch,execCount);
         Task nonblockingTask = new Task(null,execCount);

         // submit a limited task for every thread.
         for (int i = 0; i < numThreads; i++)
            executor.processMessage(blockingTask);

         // none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 0;
            }
         }));

         // now submit one more
         executor.processMessage(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // with the DefaultDempsyExecutor set to unlimited, this will
         // always be zero.
         assertEquals(0,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() { } });

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should also NOT be reflected in the "limited" count.
         assertEquals(0,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.processMessage(nonblockingTask);

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.processMessage(nonblockingTask);

         // now if I add one more, normally I should see exactly one discard.
         // with unlimited set I should see none.
         executor.processMessage(nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true);  latch.notifyAll(); }
         
         // The additional one added causes one not to count since it's rejected.
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1 + 1;
         
         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));

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
            executor.processMessage(blockingTask);

         // none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 0;
            }
         }));

         // now submit one more
         executor.processMessage(nonblockingTask);

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 1;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(1,executor.getNumberPending());

         // this should also be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to submit a non-limited and it should wait.
         executor.submit(new Runnable() { @Override public void run() { } });

         // now the pending tasks should go to one.
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberPending() == 2;
            }
         }));

         // and should stay there
         Thread.sleep(10);
         assertEquals(2,executor.getNumberPending());

         // this should NOT be reflected in the "limited" count.
         assertEquals(1,executor.getNumberLimitedPending());

         // now I should be able to overflow it. This should move the number pending right to the max allowed
         for (int i = 1; i < maxQueued; i++)
            executor.processMessage(nonblockingTask);

         // now the number limited pending should be the maxQueued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) throws Throwable
            {   
               return executor.getNumberLimitedPending() == maxQueued;
            }
         }));

         // The queue math is conservative and can handle one more before it would actually throw any away.
         // This is because the counter is decremented and then compared 'less than or equal to' with the
         // max. If this test ever fails below this point, please review that this is still the case.
         executor.processMessage(nonblockingTask);

         // now if I add one more I should see exactly one discard.
         executor.processMessage(nonblockingTask);

         // let them all go.
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
         
         // The additional one added causes one not to count since it's rejected.
         final int totalCountingExecs = numThreads + 1 + (maxQueued - 1) + 1;
         
         // wait until they all execute
         assertTrue(TestUtils.poll(baseTimeoutMillis, execCount, new TestUtils.Condition<AtomicLong>() {
            @Override public boolean conditionMet(AtomicLong c) { return totalCountingExecs == c.intValue(); }
         }));

         Thread.sleep(10);
         assertEquals(1,numRejected.intValue());

         // and eventually none should be queued
         assertTrue(TestUtils.poll(baseTimeoutMillis, executor, new TestUtils.Condition<DefaultDempsyExecutor>() {
            @Override public boolean conditionMet(DefaultDempsyExecutor executor) { return executor.getNumberPending() == 0; }
         }));
      }
      finally
      {
         synchronized(latch) { latch.set(true); latch.notifyAll(); }
      }
   }
}
