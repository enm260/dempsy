package com.nokia.dempsy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.lmax.disruptor.util.PaddedLong;

public class TestRingBufferControl
{
   private static long baseTimeoutMillis = 20000; // 20 seconds
   private static final int BUFFER_SIZE = 1024 * 8;
   private static final int MANY = 3;
   private static final int NUM_RUNS = 5;

   public static class Addup implements Runnable
   {
      protected long[] events;
      protected RingBufferConsumerControl rb;
      protected final AtomicLong finalValue = new AtomicLong(0);
      protected final AtomicLong totalMessageCount = new AtomicLong(0);
      
      public Addup(final RingBufferConsumerControl rb, final long[] events)
      {
         this.rb = rb;
         this.events = events;
      }

      public void reset() { finalValue.set(0L); totalMessageCount.set(0L); }
      
      public synchronized long getValue() { return finalValue.get(); }

      @Override
      public void run() 
      {
         long curPos = 0;
         long value = 0;
         long numMessages = 0;
         while (true)
         {
            final long availableTo = rb.availableTo();
            if (availableTo == RingBufferControl.ACQUIRE_STOP_REQUEST)
               break;
            while (curPos <= availableTo)
            {
               value += events[rb.index(curPos++)];
               numMessages++;
            }
            rb.notifyProcessed();
         }
         
         finalValue.set(value);
         totalMessageCount.set(numMessages);
      }
   }

   public static class Worker extends Addup
   {
      public Worker(final RingBufferConsumerControl rb, final long[] events)
      {
         super(rb,events);
      }

      @Override
      public void run() 
      {
         long value = 0;
         long numMessages = 0;
         while (true)
         {
            final long availableTo = rb.availableTo();
            if (availableTo == RingBufferControl.ACQUIRE_STOP_REQUEST)
               break;
            value += events[rb.index(availableTo)];
            numMessages++;
            rb.notifyProcessed();
         }
         
         finalValue.set(value);
         totalMessageCount.set(numMessages);
      }
   }
   public static class Publisher implements Runnable
   {
      private final PaddedLong value = new PaddedLong(0);
      private long[] events;
      private RingBufferControl rbc;
      private final AtomicLong finalValue = new AtomicLong(0);
      private long iterations = -1;
      private int start = -1;
      
      public Publisher(final RingBufferControl rbc, final long[] events)
      {
         this.rbc = rbc;
         this.events = events;
      }

      public void reset(int start, long iterations) 
      {
         value.set(0L);
         finalValue.set(0L);
         this.start = start;
         this.iterations = iterations;
      }
      
      public synchronized long getValue() { return finalValue.get(); }

      @Override
      public void run() 
      {
         Random random = new Random();
         
         // scramble the data in the events
         for (int i = 0; i < BUFFER_SIZE; i++)
            events[i] = (long)random.nextInt() ^ (long)(random.nextInt() << 32);

         long next = 0;
         final long end = start + iterations;
         
         for (long i = start; i < end; i++)
         {
            next = rbc.claim(1);
            events[rbc.index(next)] = i;
            rbc.publish(next);
         }

         rbc.publishStop();
      }
   }

   private long result(long start, long numTerms)
   {
      // heh!, Gauss was a genius. Funny, my 16 year old knew this formula off the top of his head.
      return (((start << 1) + numTerms - 1L) * numTerms) >> 1;
   }
   
   @Test
   public void test1To1RingBufferControl() throws Throwable
   {
      System.out.println("1-to-1");
      
      final long[] events = new long[BUFFER_SIZE];

      Random random = new Random();
      long iterations = 1000L * 1000L * 100L;
      
      // scramble the data in the events
      for (int i = 0; i < BUFFER_SIZE; i++)
         events[i] = (long)random.nextInt() ^ (long)(random.nextInt() << 32);
      
      RingBufferControl rbc = new RingBufferControl(BUFFER_SIZE);
      Addup runnable = new Addup(rbc,events);
      
      for (int i = 0; i < NUM_RUNS; i++)
      {
         // select a random start point
         int start = random.nextInt(1000);
         long iterationsPermutation = random.nextInt(2000) - 1000;

         long fiters = iterations + iterationsPermutation;
         runnable.reset();
         long timeMillis = run1To1RingBufferControl(start,fiters,rbc,runnable, events);
         assertEquals(result(start, fiters), runnable.getValue());
         System.out.format("%,d ops/sec%n",(long)((iterations * 1000)/timeMillis));
      }
   }

   public long run1To1RingBufferControl(final int start, final long iterations,
         final RingBufferControl rbc, final Runnable runnable, 
         final long[] events) throws Exception
   {
      Thread t = new Thread(runnable,"Consumer Thread for 1-to-1");
      t.start();
      Thread.yield();
      
      long next = 0;
      long end = start + iterations;
      
      long startTime = System.currentTimeMillis();
      for (long i = start; i < end; i++)
      {
         next = rbc.claim(1);
         events[rbc.index(next)] = i;
         rbc.publish(next);
      }

      rbc.publishStop();
      t.join(baseTimeoutMillis);
      assertFalse(t.isAlive());
      return System.currentTimeMillis() - startTime;
   }
   
   @Test
   public void testRingBufferControlWorker() throws Throwable
   {
      System.out.println("Workers, 1-to-" + MANY);
      final long[] events = new long[BUFFER_SIZE];

      Random random = new Random();
      long iterations = 1000L * 1000L * 10L;
      
      // scramble the data in the events
      for (int i = 0; i < BUFFER_SIZE; i++)
         events[i] = (long)random.nextInt() ^ (long)(random.nextInt() << 32);
      
      RingBufferControlWorkerPool rbc = new RingBufferControlWorkerPool(MANY,BUFFER_SIZE);
      Worker[] runnables = new Worker[MANY];
      for (int i = 0; i < MANY; i++)
         runnables[i] = new Worker(rbc.get(i),events);
      
      for (int i = 0; i < NUM_RUNS; i++)
      {
         // select a random start point
         int start = random.nextInt(1000);
         long iterationsPermutation = random.nextInt(2000) - 1000;

         long fiters = iterations + iterationsPermutation;
         for (int j = 0; j < MANY; j++)
            runnables[j].reset();
         long timeMillis = runRingBufferControlWorker(start,fiters,rbc,runnables, events);
         long sumMessages = 0;
         for (int j = 0; j < MANY; j++)
            sumMessages += runnables[j].getValue();
         assertEquals(result(start, fiters), sumMessages);
         System.out.format("%,d ops/sec%n",(long)((iterations * 1000)/timeMillis));
      }
   }

   public long runRingBufferControlWorker(final int start, final long iterations,
         final RingBufferControlWorkerPool rbc, final Worker[] runnables, 
         final long[] events) throws Exception
   {
      Thread[] t = new Thread[runnables.length];
      for (int i = 0; i < runnables.length; i++)
         (t[i] = new Thread(runnables[i],"Consumer Thread for 1-to-1")).start();
      Thread.yield();
      
      long next = 0;
      long end = start + iterations;
      
      long startTime = System.currentTimeMillis();
      for (long i = start; i < end; i++)
      {
         next = rbc.next();
         events[rbc.index(next)] = i;
         rbc.publish(next);
      }

      rbc.publishStop();
      for (int i = 0; i < runnables.length; i++)
      {
         t[i].join(baseTimeoutMillis);
         assertFalse(t[i].isAlive());
      }
      return System.currentTimeMillis() - startTime;
   }

   @Test
   public void testRingBufferControlMulticaster() throws Throwable
   {
      System.out.println("Multicast, 1-to-" + MANY);
      final long[] events = new long[BUFFER_SIZE];

      Random random = new Random();
      long iterations = 1000L * 1000L * 100L;
      
      // scramble the data in the events
      for (int i = 0; i < BUFFER_SIZE; i++)
         events[i] = (long)random.nextInt() ^ (long)(random.nextInt() << 32);
      
      RingBufferControlMulticaster rbc = new RingBufferControlMulticaster(MANY,BUFFER_SIZE);
      Addup[] runnables = new Addup[MANY];
      for (int i = 0; i < MANY; i++)
         runnables[i] = new Addup(rbc.get(i),events);
      
      for (int i = 0; i < NUM_RUNS; i++)
      {
         // select a random start point
         int start = random.nextInt(1000);
         long iterationsPermutation = random.nextInt(2000) - 1000;

         long fiters = iterations + iterationsPermutation;
         for (int j = 0; j < MANY; j++)
            runnables[j].reset();
         long timeMillis = runRingBufferControlMulticaster(start,fiters,rbc,runnables, events);
         for (int j = 0; j < MANY; j++)
            assertEquals(result(start, fiters), runnables[j].getValue());
         System.out.format("%,d ops/sec%n",(long)((iterations * 1000)/timeMillis));
      }
   }

   public long runRingBufferControlMulticaster(final int start, final long iterations,
         final RingBufferControlMulticaster rbc, final Addup[] runnables, 
         final long[] events) throws Exception
   {
      Thread[] t = new Thread[runnables.length];
      for (int i = 0; i < runnables.length; i++)
         (t[i] = new Thread(runnables[i],"Consumer Thread for 1-to-1")).start();
      Thread.yield();
      
      long next = 0;
      long end = start + iterations;
      
      long startTime = System.currentTimeMillis();
      for (long i = start; i < end; i++)
      {
         next = rbc.claim(1);
         events[rbc.index(next)] = i;
         rbc.publish(next);
      }

      rbc.publishStop();
      for (int i = 0; i < runnables.length; i++)
      {
         t[i].join(baseTimeoutMillis);
         assertFalse(t[i].isAlive());
      }
      return System.currentTimeMillis() - startTime;
   }
   
   @Test
   public void testRingBufferControlMultiplexor() throws Throwable
   {
      System.out.println("Multiplexor, " + MANY + "-to-1");
      
      final long[][] events = new long[MANY][BUFFER_SIZE];

      Random random = new Random();
      final long iterations = 1000L * 1000L * 100L;
      
      // scramble the data in the events
      for (int j = 0; j < MANY; j++)
         for (int i = 0; i < BUFFER_SIZE; i++)
            events[j][i] = (long)random.nextInt() ^ (long)(random.nextInt() << 32);
      
      RingBufferControlMultiplexor rbc = new RingBufferControlMultiplexor(BUFFER_SIZE,(Object[])events);
      Publisher[] publishers = new Publisher[MANY];
      for (int i = 0; i < MANY; i++)
         publishers[i] = new Publisher(rbc.get(i), events[i]);
      
      for (int j = 0; j < NUM_RUNS; j++)
      {
         long finalValue = 0;
         long totalIterations = 0;
         for (int i = 0; i < MANY; i++)
         {
            // select a random start point
            final int start = random.nextInt(1000);
            final long iterationsPermutation = random.nextInt(2000) - 1000;
            final long fiters = iterations + iterationsPermutation;
            totalIterations += fiters;
            finalValue += result(start, fiters);
            publishers[i].reset(start, fiters);
         }

         final PaddedLong receievedCount = new PaddedLong(0);
         final long timeMillis = runManyTo1RingBufferControl(rbc, receievedCount, publishers);
         assertEquals(finalValue,receievedCount.get());
         System.out.format("%,d ops/sec%n",(long)((totalIterations * 1000)/timeMillis));
      }
   }

   public long runManyTo1RingBufferControl(
         RingBufferControlMultiplexor rb, final PaddedLong value, final Runnable... publishers) throws Exception
   {
      long[] curPoss = new long[publishers.length];
      Thread[] t = new Thread[publishers.length];
      for (int i = 0; i < publishers.length; i++)
      {
         (t[i] = new Thread(publishers[i],"Producer Thread for *-to-1")).start();
         curPoss[i] = 0;
      }
      
      long startTime = System.currentTimeMillis();
      while (true)
      {
         final long availableTo = rb.availableTo();
         if (availableTo == RingBufferControl.ACQUIRE_STOP_REQUEST)
            break;
         long curValue = value.get();
         final int curIndex = rb.getCurrentIndex();
         long curPos = curPoss[curIndex];
         while (curPos <= availableTo)
            curValue += ((long[])rb.getCurrentControlled())[rb.index(curPos++)];
         rb.notifyProcessed();
         curPoss[curIndex] = curPos;
         value.set(curValue);
      }
      final long ret = System.currentTimeMillis() - startTime;

      for (int i = 0; i < publishers.length; i++)
      {
         t[i].join(baseTimeoutMillis);
         assertFalse(t[i].isAlive());
      }
      
      return ret;
   }

}
