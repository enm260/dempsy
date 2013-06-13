/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * This code is substantially based on the ingenious work done by Martin 
 * Thompson on what he calls "Mechanical Sympathy." It leans heavily on 
 * the source code from version 3.0.0.beta2 of the LMAX-exchange Disruptor
 * but has been completely refactored in order to invert separate the control
 * mechanism from what is being controlled and to simplify the API.
 * 
 * For more information on the LMAX Disruptor, see:
 * 
 *      http://lmax-exchange.github.com/disruptor/
 */

package com.nokia.dempsy.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.util.PaddedLong;

/**
 * <p>This is a helper class for managing a set of {@link RingBufferControl}s for use
 * in a "single-publisher to multi-consumer" thread configuration where the consumers
 * are workers reading from the queued data.</p> 
 * 
 * <p>Currently it would be really bad for Worker consumers to mix availableTo and tryAvailableTo.
 * If a worker uses tryAvailableTo it MUST use tryAvailableTo until tryAvailableTo returns a
 * value that isn't RingBufferConsumerControl.UNAVAILABLE before using availableTo.</p>
 */
public class RingBufferControlWorkerPool
{
   private final Sequence cursor = new Sequence(RingBufferConsumerControl.INITIAL_CURSOR_VALUE);
   private final Sequence workSequence = new Sequence(RingBufferConsumerControl.INITIAL_CURSOR_VALUE);
   
   private final RingBufferConsumerControl[] consumers;
   private final Sequence[] tails;
   private final int bufferSize; 
   private final int indexMask;
   private final AtomicInteger openCount = new AtomicInteger(0);
   
   @SuppressWarnings("unused")
   private static class Padding
   {
       /** Set to -1 as sequence starting point */
       public long nextValue = RingBufferConsumerControl.INITIAL_CURSOR_VALUE, 
             tailCache = RingBufferConsumerControl.INITIAL_CURSOR_VALUE,
             p2, p3, p4, p5, p6, p7;
   }

   // tail cache accessed from the publish side only.
   // head cache accessed from the publish side only
   private final Padding pubHeadAndTailCache = new Padding();
   
   public RingBufferControlWorkerPool(int numberOfSubscribers, int sizePowerOfTwo)
   {
      this(numberOfSubscribers,sizePowerOfTwo,RingBufferConsumerControl.yield);
   }

   public RingBufferControlWorkerPool(int numberOfSubscribers, int sizePowerOfTwo, RingBufferConsumerControl.ConsumerWaitStrategy waitStrategy)
         throws IllegalArgumentException
   {
      this.consumers = new RingBufferConsumerControl[numberOfSubscribers];
      this.tails = new Sequence[numberOfSubscribers];
      
      for (int i = 0; i < numberOfSubscribers; i++)
      {
         this.consumers[i] = new RingBufferConsumerControl(sizePowerOfTwo, waitStrategy, cursor)
         {
            // save off the workSequence ... shared among all workers.
            private final Sequence workSequence = RingBufferControlWorkerPool.this.workSequence;
            
            @Override
            protected void clear()
            {
               int curCount = openCount.decrementAndGet();
               if (curCount == 0)
               {
                  cursor.set(RingBufferConsumerControl.INITIAL_CURSOR_VALUE);
                  openCount.set(consumers.length);
                  pubHeadAndTailCache.nextValue = RingBufferConsumerControl.INITIAL_CURSOR_VALUE;
                  pubHeadAndTailCache.tailCache = RingBufferConsumerControl.INITIAL_CURSOR_VALUE;
                  workSequence.set(RingBufferConsumerControl.INITIAL_CURSOR_VALUE);
               }
               super.clear();
            }
            
            @Override
            public long availableTo()
            {
               // humm ... no getAndIncrement.
               final long ret = workSequence.incrementAndGet();
               super.doNotifyProcessed(ret - 1L); // notify up to the previous
               final long alt = super.availableTo(ret);
               return alt == RingBufferConsumerControl.ACQUIRE_STOP_REQUEST ? alt : ret;
            }
            
            final PaddedLong allocatedTry = new PaddedLong(INITIAL_CURSOR_VALUE);
            // TODO: add a test for this.
            @Override
            public long tryAvailableTo()
            {
               final boolean inProcess = allocatedTry.get() != INITIAL_CURSOR_VALUE;
               final long ret = inProcess ? allocatedTry.get() : workSequence.incrementAndGet();
               if (!inProcess)
               {
                  allocatedTry.set(ret);
                  super.doNotifyProcessed(ret - 1L);
               }
               
               final long alt = super.tryAvailableTo(ret);
               if (alt == RingBufferConsumerControl.UNAVAILABLE)
                  return alt;
               else if (inProcess) // reset the allocatedTry
                  allocatedTry.set(INITIAL_CURSOR_VALUE); // reset the allocatedTry
               return alt == RingBufferConsumerControl.ACQUIRE_STOP_REQUEST ? alt : ret;
            }
            
            @Override
            public void notifyProcessed() { /* We don't do anything here. This is done in availableTo/tryAvailableTo.*/ }
         };
         
         tails[i] = this.consumers[i].getTail();
      }
      this.openCount.set(numberOfSubscribers);
      this.indexMask = sizePowerOfTwo - 1;
      this.bufferSize = sizePowerOfTwo;
   }
   
   /**
    * This will retrieve the {@link RingBufferControl} that corresponds to the 
    * index given. This is the way a particular subscriber should retrieve its
    * corresponding {@link RingBufferControl}.
    */
   public RingBufferConsumerControl get(final int index) { return consumers[index]; }
   
   /**
    * This is used by the publishing thread to claim the given number of entries
    * in the buffer all of the underlying {@link RingBufferControl}s. The sequence
    * returned should be supplied to the {@link RingBufferControlWorkerPool#publish(long)}
    * command once the publisher thread has prepared the entries.
    * 
    * @param requestedNumberOfSlots is the number of entries in the buffer we need
    * to wait for to be open.
    * @return the sequence to provide to the {@link RingBufferControl#publish(long)}
    * or the {@link RingBufferControl#index(long)} methods.
    */
   public long next()
   {
      final long curNextValue = pubHeadAndTailCache.nextValue;
      final long nextSequence = curNextValue + 1L;
      final long wrapPoint = nextSequence - bufferSize;
      final long cachedGatingSequence = pubHeadAndTailCache.tailCache;
      
      if (wrapPoint > cachedGatingSequence || cachedGatingSequence > curNextValue)
      {
          long minSequence;
          while (wrapPoint > (minSequence = getMinimumSequence(tails, curNextValue)))
              LockSupport.parkNanos(1L);
      
          pubHeadAndTailCache.tailCache = minSequence;
      }
      
      pubHeadAndTailCache.nextValue = nextSequence;
      
      return nextSequence;
   }
   
   /**
    * This is used by the publishing thread to claim the next entry
    * in the buffer all of the underlying {@link RingBufferControl}s. The sequence
    * returned should be supplied to the {@link RingBufferControlWorkerPool#publish(long)}
    * command once the publisher thread has prepared the entries.
    * 
    * If there are no currently available slots in the RingBuffer then 
    * RingBufferControl.UNAVAILABLE will be returned.
    * 
    * @param requestedNumberOfSlots is the number of entries in the buffer we need
    * to wait for to be open.
    * @return the sequence to provide to the {@link RingBufferControl#publish(long)}
    * or the {@link RingBufferControl#index(long)} methods.
    */
   public long tryNext()
   {
      final long curNextValue = pubHeadAndTailCache.nextValue;
      final long nextSequence = curNextValue + 1L;
      final long wrapPoint = nextSequence - bufferSize;
      final long cachedGatingSequence = pubHeadAndTailCache.tailCache;
      
      if (wrapPoint > cachedGatingSequence || cachedGatingSequence > curNextValue)
      {
          final long minSequence = getMinimumSequence(tails, curNextValue);
          if (wrapPoint > minSequence)
              return RingBufferControl.UNAVAILABLE;
      
          pubHeadAndTailCache.tailCache = minSequence;
      }
      
      pubHeadAndTailCache.nextValue = nextSequence;
      
      return nextSequence;
   }

   /**
    * Once the publisher has readied the buffer entries that were claimed, this method
    * allows the subscribers to be notified that they are ready.
    * @param sequence is the sequence returned from the
    * {@link RingBufferControlWorkerPool#claim(int)} call.
    */
   public void publish(final long sequence)
   {
      cursor.set(sequence);
   }
   
   /**
    * <p>The {@link RingBufferControl} can ONLY be stopped from the publish side. The publisher
    * needs to call publishStop to stop the consumer. Once the consumer reaches this point
    * in the sequence, the consumer will receive a {@link RingBufferControl#ACQUIRE_STOP_REQUEST}
    * returned from either {@link RingBufferControl#availableTo()} or 
    * {@link RingBufferControl#tryAvailableTo()}.</p>
    * 
    * <p>Once that happens the {@link RingBufferControl#isShutdown()} will return <code>true</code> 
    * on both the publisher and consumer sides.</p>
    * 
    * @return the sequence that represents where the consumer will be notified to stop.
    */
   public long publishStop()
   {
      long next = next();
      for (int i = 0; i < consumers.length; i++)
         consumers[i].stop.set(next);
      for (int i = 1; i < consumers.length; i++)
         next = next();
      
      publish(next);
      return next;
   }
   
   /**
    * This method will convert the sequence to an index of a ring buffer. 
    */
   public int index(final long sequence) { return (int)sequence & indexMask; }
   
   /**
    * Once the publisher calls {@link RingBufferControl#publishStop()} and the 
    * consumer acquires it this method will return <code>true</code>. It will
    * also return <code>true</code> up until the first sequence is retrieved
    * by a consumer. It will return <code>false</code> at all other times.
    */
   public boolean isShutdown() { return cursor.get() == RingBufferConsumerControl.INITIAL_CURSOR_VALUE; }
   
   public int getBufferSize() { return bufferSize; }
   
   /**
    * This is an estimate of the number of entries currently in the RingBuffer.
    */
   public long getNumEntries()
   {
      // If the client a worker is waiting in an availableTo call then the
      // workerSequence will be ahead of the pubHeadAndTailCache.nextValue.
      final long ret = pubHeadAndTailCache.nextValue - workSequence.get();
      return ret < 0 ? 0 : ret;
   }
   
   /**
    * Get the minimum sequence from an array of {@link com.lmax.disruptor.Sequence}s.
    *
    * @param sequences to compare.
    * @param minimum an initial default minimum.  If the array is empty this value will be
    * returned.
    * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
    */
   private static long getMinimumSequence(final Sequence[] sequences, long minimum)
   {
       for (int i = 0, n = sequences.length; i < n; i++)
       {
           long value = sequences[i].get();
           minimum = Math.min(minimum, value);
       }

       return minimum;
   }
}
