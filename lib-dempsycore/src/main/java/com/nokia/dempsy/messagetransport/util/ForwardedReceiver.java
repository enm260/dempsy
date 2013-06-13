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

package com.nokia.dempsy.messagetransport.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.message.MessageBufferInput;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Receiver;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.util.PaddedCount;

public class ForwardedReceiver implements Receiver
{
   private static Logger logger = LoggerFactory.getLogger(ForwardedReceiver.class);
   
   protected final DempsyExecutor executor;
   protected final Server server;
   protected final AtomicBoolean isStarted = new AtomicBoolean(false);
   protected final boolean iShouldStopExecutor;
   
   protected ReceiverIndexedDestination destination;
   private String destinationString = "";

   private Listener messageTransportListener;
   private boolean failFast;
   
   protected StatsCollector statsCollector;

   public ForwardedReceiver(final Server server, final DempsyExecutor executor, 
         final boolean failFast, final boolean stopExecutor) throws MessageTransportException 
   {
      this.server = server;
      this.executor = executor;
      this.failFast = failFast;
      if (executor == null)
         throw new MessageTransportException("You must set the executor on a " + 
               ForwardedReceiver.class.getSimpleName());
      this.iShouldStopExecutor = stopExecutor;
   }
   
   public DempsyExecutor getExecutor() { return executor; }
   
   public Server getServer() { return server; }
   
   @Override
   public synchronized void start() throws MessageTransportException
   {
      if (isStarted())
         return;
      
      getDestination();
      
      setPendingGague();
      
      isStarted.set(true);
   }
   
   @Override
   public boolean getFailFast() { return failFast; }
   
   protected void handleMessage(final MessageBufferInput msg)
   {
      if ( messageTransportListener != null)
      {
         try
         {
            executor.processMessage(new DempsyExecutor.Rejectable()
            {
               final MessageBufferInput message = msg;

               @Override
               public void run()
               {
                  try { messageTransportListener.onMessage( message, failFast ); }
                  catch (Throwable th) 
                  {
                     if (statsCollector != null) statsCollector.messageFailed(true);
                     logger.error(SafeString.objectDescription(messageTransportListener) + " failed to process message.",th);
                  }
               }

               @Override
               public void rejected()
               {
                  if (statsCollector != null)
                     statsCollector.messageDiscarded(message);
               }
            });
            
         }
         catch (Throwable se)
         {
            String messageAsString;
            try { messageAsString = (msg == null ? "null" : msg.getBuffer().toString()); } catch (Throwable th) { messageAsString = "(failed to convert message to a string)"; }
            logger.error("Unexpected listener exception on adaptor for " + destinationString +
                  " trying to process a message of with a value of " +  messageAsString + " using listener " + 
                  SafeString.valueOfClass(messageTransportListener.getClass().getSimpleName()), se);
         }
      }
   }
   
   public synchronized void shutdown()
   {
      if (destination != null) server.unregister(destination);
      
      try { if ( messageTransportListener != null) messageTransportListener.transportShuttingDown(); }
      catch (Throwable th)
      { logger.error("Listener threw exception when being notified of shutdown on " + destination, th); }
      
      if (executor != null && iShouldStopExecutor)
         executor.shutdown();

      isStarted.set(false);
   }
   
   public synchronized boolean isStarted() { return isStarted.get(); }
   
   @Override
   public synchronized ReceiverIndexedDestination getDestination() throws MessageTransportException
   {
      if (destination == null)
         destination = server.register(this);
      
      destinationString = destination.toString();

      return destination;
   }
   
   @Override
   public void setListener(Listener messageTransportListener )
   {
      this.messageTransportListener = messageTransportListener;
   }
   
   @Override
   public synchronized void setStatsCollector(StatsCollector statsCollector) 
   {
      this.statsCollector = statsCollector;
      setPendingGague();
   }
   
   protected void setPendingGague()
   {
      if (statsCollector != null && executor != null)
      {
         statsCollector.setMessagesPendingGauge(new StatsCollector.Gauge()
         {
            @Override
            public long value()
            {
               return executor.getNumberPending();
            }
         });
      }
   }
}
