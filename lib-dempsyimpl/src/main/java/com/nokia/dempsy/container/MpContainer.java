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

package com.nokia.dempsy.container;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import com.nokia.dempsy.Dispatcher;
import com.nokia.dempsy.KeySource;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.container.internal.AnnotatedMethodInvoker;
import com.nokia.dempsy.container.internal.LifecycleHelper;
import com.nokia.dempsy.executor.DempsyExecutor;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Listener;
import com.nokia.dempsy.monitoring.StatsCollector;
import com.nokia.dempsy.output.OutputInvoker;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;
import com.nokia.dempsy.serialization.java.JavaSerializer;

/**
 *  <p>The {@link MpContainer} manages the lifecycle of message processors for the
 *  node that it's instantiated in.</p>
 *  
 *  The container is simple in that it does no thread management. When it's called 
 *  it assumes that the transport has provided the thread that's needed 
 */
public class MpContainer implements Listener, OutputInvoker, RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener
{
   private Logger logger = LoggerFactory.getLogger(getClass());

   // these are set during configuration
   private LifecycleHelper prototype;
   private Dispatcher dispatcher;

   // Note that this is set via spring/guice.  Tests that need it,
   // should set it them selves.  Having a default leaks 8 threads per
   // instance.
   private StatsCollector statCollector;

   // default instance optionally replaced via dependency injection
   private Serializer<Object> serializer = new JavaSerializer<Object>();

   // this is used to retrieve message keys
   private AnnotatedMethodInvoker keyMethods = new AnnotatedMethodInvoker(MessageKey.class);

   // message key -> instance that handles messages with this key
   // changes to this map will be synchronized; read-only may be concurrent
   private ConcurrentHashMap<Object,InstanceWrapper> instances = new ConcurrentHashMap<Object,InstanceWrapper>();

   // Scheduler to handle eviction thread.
   private ScheduledExecutorService evictionScheduler;

   // The ClusterId is set for the sake of error messages.
   private ClusterId clusterId;
   
   // This holds the keySource for pre(re)-instantiation
   private KeySource<?> keySource = null;

   private volatile boolean isRunning = true;
   
   private DempsyExecutor executor = null;
   
   private RoutingStrategy.Inbound strategyInbound = null;

   public MpContainer(ClusterId clusterId) { this.clusterId = clusterId; }

   protected static class InstanceWrapper
   {
      private Object instance;
      private Semaphore lock = new Semaphore(1,true); // basically a mutex
      private boolean evicted = false;

      /**
       * DO NOT CALL THIS WITH NULL OR THE LOCKING LOGIC WONT WORK
       */
      public InstanceWrapper(Object o) { this.instance = o; }

      /**
       * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
       * @param block - whether or not to wait for the lock.
       * @return the instance if the lock was aquired. null otherwise.
       */
      public Object getExclusive(boolean block)
      {
         if (block)
         {
            boolean gotLock = false;
            while (!gotLock)
            {
               try { lock.acquire(); gotLock = true; } catch (InterruptedException e) { }
            }
         }
         else
         {
            if (!lock.tryAcquire())
               return null;
         }

         // if we got here we have the lock
         return instance;
      }

      /**
       * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
       * MAKE SURE YOU OWN THE LOCK IF YOU UNLOCK IT.
       */
      public void releaseLock()
      {
         lock.release();
      }

      public boolean tryLock()
      {
         return lock.tryAcquire();
      }
      
//      /**
//       * This will set the instance reference to null. MAKE SURE
//       * YOU OWN THE LOCK BEFORE CALLING.
//       */
//      public void markPassivated() { instance = null; }
//
//      /**
//       * This will tell you if the instance reference is null. MAKE SURE
//       * YOU OWN THE LOCK BEFORE CALLING.
//       */
//      public boolean isPassivated() { return instance == null; }


      /**
       * This will prevent further operations on this instance. 
       * MAKE SURE YOU OWN THE LOCK BEFORE CALLING.
       */
      public void markEvicted() { evicted = true; }

      /**
       * Flag to indicate this instance has been evicted and no further operations should be enacted.
       * THIS SHOULDN'T BE CALLED WITHOUT HOLDING THE LOCK.
       */
      public boolean isEvicted() { return evicted; }

      //----------------------------------------------------------------------------
      //  Test access
      //----------------------------------------------------------------------------
      protected Object getInstance() { return instance; }
   }

   //----------------------------------------------------------------------------
   //  Configuration
   //----------------------------------------------------------------------------

   public void setPrototype(Object prototype) throws ContainerException
   {
      this.prototype = new LifecycleHelper(prototype);
      
      preInitializePrototype(prototype);
      
      if (outputConcurrency > 0)
         setupOutputConcurrency();
   }
   
   @Override
   public void setInboundStrategy(RoutingStrategy.Inbound strategyInbound) { this.strategyInbound = strategyInbound; }
   
   /**
    * Run any methods annotated PreInitilze on the MessageProcessor prototype
    * @param prototype reference to MessageProcessor prototype
    */
   private void preInitializePrototype(Object prototype) {
      for (Method method: prototype.getClass().getMethods()) {
         if (method.isAnnotationPresent(com.nokia.dempsy.annotations.Start.class)) {
            // if the start method takes a ClusterId or ClusterDefinition then pass it.
            Class<?>[] parameterTypes = method.getParameterTypes();
            boolean takesClusterId = false;
            if (parameterTypes != null && parameterTypes.length == 1)
            {
               if (ClusterId.class.isAssignableFrom(parameterTypes[0]))
                  takesClusterId = true;
               else
               {
                  logger.error("The method \"" + method.getName() + "\" on " + SafeString.objectDescription(prototype) + 
                        " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " + 
                        "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter."); 
                  
                  return; // return without invoking start.
               }
            }
            else if (parameterTypes != null && parameterTypes.length > 1)
            {
               logger.error("The method \"" + method.getName() + "\" on " + SafeString.objectDescription(prototype) + 
                     " is annotated with the @" + Start.class.getSimpleName() + " annotation but doesn't have the correct signature. " + 
                     "It needs to either take no parameters or take a single " + ClusterId.class.getSimpleName() + " parameter."); 
               return; // return without invoking start.
            }
            try {
               if (takesClusterId)
                  method.invoke(prototype, clusterId);
               else
                  method.invoke(prototype);
               break;  // Only allow one such method, which is checked during validation
            }catch(Exception e) {
               logger.error(MarkerFactory.getMarker("FATAL"), "can't run MP initializer " + method.getName(), e);
            }
         }
      }
   }


   public Object getPrototype()
   {
      return prototype == null ? null : prototype.getPrototype();
   }

   public LifecycleHelper getLifecycleHelper() {
      return prototype;
   }

   public Map<Object,InstanceWrapper> getInstances() {
      return instances;
   }

   public void setDispatcher(Dispatcher dispatcher)
   {
      this.dispatcher = dispatcher;
   }
   
   public void setExecutor(DempsyExecutor executor)
   {
      this.executor = executor;
   }

   /**
    * Set the StatsCollector.  This is optional, but likely desired in
    * a production environment.  The default is to use the Coda Metrics 
    * StatsCollector with only the default JMX reporters.  Production
    * environments will likely want to export stats to a centralized
    * monitor like Graphite or Ganglia.
    */
   public void setStatCollector(StatsCollector collector)
   {
      this.statCollector = collector;
   }

   /**
    * Set the serializer.  This can be injected to change the serialization
    * scheme, but is optional.  The default serializer uses Java Serialization.
    * 
    * @param serializer
    */
   public void setSerializer(Serializer<Object> serializer) { this.serializer = serializer; }

   public Serializer<Object> getSerializer() { return this.serializer; }
   
   public void setKeySource(KeySource<?> keySource) { this.keySource = keySource; }


   //----------------------------------------------------------------------------
   //  Monitoring / Management
   //----------------------------------------------------------------------------


   //----------------------------------------------------------------------------
   //  Operation
   //----------------------------------------------------------------------------

   /**
    *  {@inheritDoc}
    *  @return true, to acknowledge the message.
    */
   @Override
   public boolean onMessage(byte[] data, boolean fastfail)
   {
      Object message = null;
      try
      {
         message = serializer.deserialize(data);
         statCollector.messageReceived(data);
         return dispatch(message,!fastfail);
      }
      catch(SerializationException e2)
      {
         logger.warn("the container for " + clusterId + " failed to deserialize message received for " + clusterId, e2);
      }
      catch (ContainerException ce)
      {
         if (ce.isExpected())
         {
            if (logger.isDebugEnabled())
               logger.debug("the container for " + clusterId + " failed to dispatch message the following message " + 
               SafeString.objectDescription(message) + " to the message processor " + SafeString.valueOf(prototype),ce);
         }
         else
            logger.warn("the container for " + clusterId + " failed to dispatch message the following message " + 
                  SafeString.objectDescription(message) + " to the message processor " + SafeString.valueOf(prototype), ce);
      }
      catch (Throwable ex)
      {
         logger.warn("the container for " + clusterId + " failed to dispatch message the following message " + 
               SafeString.objectDescription(message) + " to the message processor " + SafeString.valueOf(prototype), ex);
      }

      return false;
   }


   /**
    *  {@inheritDoc}
    */
   @Override
   public void shuttingDown()
   {
      isRunning = false;
      shutdown();
      // we don't care about the transport shutting down (currently)

   }

   public void shutdown()
   {      
      if (evictionScheduler != null)
         evictionScheduler.shutdownNow();

      // the following will close up any output executor that might be running
      setConcurrency(-1);
   }


   //----------------------------------------------------------------------------
   // Monitoring and Management
   //----------------------------------------------------------------------------

   /**
    *  Returns the number of message processors controlled by this manager.
    */
   public int getProcessorCount()
   {
      return instances.size();
   }
   
   /**
    * This is a helper class with unique synchronizataion behavior. It can be used to start
    * a worker in another thread, allow the initiating thread to verify that's it's been 
    * started, and allow the early termination of that worker thread.
    */
   private class RunningEventSwitch
   {
      final AtomicBoolean isRunning = new AtomicBoolean(false);
      final AtomicBoolean stopRunning = new AtomicBoolean(false);
      // this flag is used to hold up the calling thread's exit of this method
      //  until the worker is underway.
      final AtomicBoolean runningGate = new AtomicBoolean(false);
      
      /**
       * This is called from the worker thread to notify the fact that
       * it's been started.
       */
      public void workerInitiateRun()
      {
         // this is synchronized because it's used as a condition variable 
         // along with the condition.
         synchronized(isRunning) { isRunning.set(true); }
         stopRunning.set(false);
         
         synchronized(runningGate)
         {
            runningGate.set(true);
            runningGate.notify();
         }
      }
      
      /**
       * The worker thread can use this method to check if it's been explicitly preempted.
       * @return
       */
      public boolean wasPreempted() { return stopRunning.get(); }
      
      /**
       * The worker thread should indicate that it's done in a finally clause on it's way
       * out.
       */
      public void workerStopping()
      {
         // This kicks the preemptWorkerAndWait out.
         synchronized(isRunning)
         {
            isRunning.set(false);
            isRunning.notify();
         }
      }
      
      /**
       * The main thread uses this method when it needs to preempt the worker and
       * wait for the worker to finish before continuing.
       */
      public void preemptWorkerAndWait()
      {
         // We need to see if we're already executing
         stopRunning.set(true); // kick out any running instantiation thread
         // wait for it to exit, it it's even running - also consider the overall
         //  Mp isRunning flag.
         synchronized(isRunning)
         {
            while (isRunning.get() && MpContainer.this.isRunning)
            {
               try { isRunning.wait(); } catch (InterruptedException e) {}
            }
         }
      }
      
      /**
       * This allows the main thread to wait until the worker is started in order
       * to continue. This method only works once. It resets the flag so a second
       * call will block until another thread calls to workerInitiateRun().
       */
      public void waitForWorkerToStart()
      {
         // make sure the thread is running before we head out from the synchronized block
         synchronized(runningGate)
         {
            while (runningGate.get() == false && MpContainer.this.isRunning)
            {
               try { runningGate.wait(); } catch (InterruptedException ie) {}
            }

            runningGate.set(false); // reset this flag
         }
      }
   }
   
   final RunningEventSwitch expand = new RunningEventSwitch();
   final RunningEventSwitch contract = new RunningEventSwitch();
   final Object keyspaceResponsibilityChangedLock = new Object(); // we need to synchronize keyspaceResponsibilityChanged alone
   
   private void runExpandKeyspace()
   {
      List<Future<Object>> futures = new ArrayList<Future<Object>>();
      expand.workerInitiateRun();

      StatsCollector.TimerContext tcontext = null;
      try{
         tcontext = statCollector.preInstantiationStarted();
         Iterable<?> iterable = keySource.getAllPossibleKeys();
         for(final Object key: iterable)
         {
            if (expand.wasPreempted() || !isRunning)
               break;
            
            // strategyInbound can't be null if we're in runExpandKeyspace since it was invoked 
            //  indirectly from it. So here we don't need to check for null.
            if(strategyInbound.doesMessageKeyBelongToNode(key))
            {
               Callable<Object> callable = new Callable<Object>()
               {
                  Object k = key;
                  
                  @Override
                  public Object call()
                  {
                     try { getInstanceForKey(k); }
                     catch(ContainerException e)
                     {
                        logger.error("Failed to instantiate MP for Key "+key +
                              " of type "+key.getClass().getSimpleName(), e);
                     }
                     return null;
                  }
               };
               
               if (executor != null)
                  futures.add(executor.submit(callable));
               else
                  callable.call();
            }
         }
      }
      catch(Throwable e)
      {
         logger.error("Exception occured while processing keys during pre-instantiation using KeyStore method"+
               keySource.getClass().getSimpleName()+":getAllPossibleKeys()", e);
      }
      finally
      {
         if (tcontext != null) tcontext.stop();
         if (expand.wasPreempted()) // this run is being preempted
         {
            for (Future<Object> f : futures)
               try { f.cancel(false); } catch (Throwable th) { logger.warn("Error trying to cancel an attempt to pre-instantiate a Mp",th); }
         }
         expand.workerStopping();
      }
   }
   
   @Override
   public void keyspaceResponsibilityChanged(boolean less, boolean more)
   {
      synchronized(keyspaceResponsibilityChangedLock)
      {
         // need to handle less by passivating
         if (less)
         {
            contract.preemptWorkerAndWait();
            
            Thread t = new Thread(new Runnable()
            {
               @Override
               public void run() 
               {
                  try
                  {
                     contract.workerInitiateRun();
                     doEvict(new EvictCheck()
                     {
                        // we shouldEvict if the message key no longer belongs as 
                        //  part of this container.
                        // strategyInbound can't be null if we're here since this was invoked 
                        //  indirectly from it. So here we don't need to check for null.
                        @Override
                        public boolean shouldEvict(Object key, InstanceWrapper wrapper) { return !strategyInbound.doesMessageKeyBelongToNode(key); }
                        // In this case, it's evictable.
                        @Override
                        public boolean isGenerallyEvitable() { return true; }
                        @Override
                        public boolean shouldStopEvicting() { return contract.wasPreempted(); }
                     });
                  }
                  finally { contract.workerStopping(); }
               }
            }, "Keyspace Contraction Thread");
            t.setDaemon(true);
            t.start();

            contract.waitForWorkerToStart();
         }

         // If more were added and we have a keySource we need to do
         //  preinstantiation
         if (more && keySource != null)
         {
            expand.preemptWorkerAndWait();
            
            Thread t = new Thread(new Runnable()
            {
               @Override
               public void run() { runExpandKeyspace();  }
            }, "Pre-Instantation Thread");
            t.setDaemon(true);
            t.start();

            expand.waitForWorkerToStart();
         }
         
      }
   }

   //----------------------------------------------------------------------------
   //  Test Hooks
   //----------------------------------------------------------------------------

   protected Dispatcher getDispatcher() { return dispatcher; }

   protected StatsCollector getStatsCollector() { return statCollector; }

   /**
    *  Returns the Message Processor that is associated with a given key,
    *  <code>null</code> if there is no such MP. Does <em>not</em> create
    *  a new MP.
    *  <p>
    *  <em>This method exists for testing; don't do anything stupid</em>
    */
   protected Object getMessageProcessor(Object key)
   {
      InstanceWrapper wrapper = instances.get(key);
      return (wrapper != null) ? wrapper.getInstance() : null;
   }


   //----------------------------------------------------------------------------
   //  Internals
   //----------------------------------------------------------------------------

   // this is called directly from tests but shouldn't be accessed otherwise.
   protected boolean dispatch(Object message, boolean block) throws ContainerException {
      if (message == null)
         return false; // No. We didn't process the null message
      
      boolean evictedAndBlocking;
      boolean messageDispatchSuccessful = false;

      do
      {
         evictedAndBlocking = false;
         InstanceWrapper wrapper = getInstanceForDispatch(message);

         // wrapper cannot be null ... look at the getInstanceForDispatch method
         Object instance = wrapper.getExclusive(block);

         if (instance != null) // null indicates we didn't get the lock
         {
            try
            {
               if(wrapper.isEvicted())
               {
                  // if we're not blocking then we need to just return a failure. Otherwise we want to try again
                  // because eventually the current Mp will be passivated and removed from the container and
                  // a subsequent call to getInstanceForDispatch will create a new one.
                  if (block)
                  {
                     Thread.yield();
                     evictedAndBlocking = true; // we're going to try again.
                  }
                  else // otherwise it's just like we couldn't get the lock. The Mp is busy being killed off.
                  {
                     if (logger.isTraceEnabled())
                        logger.trace("the container for " + clusterId + " failed handle message due to evicted Mp " + SafeString.valueOf(prototype));
                     
                     statCollector.messageDiscarded(message);
                     statCollector.messageCollision(message);
                     messageDispatchSuccessful = false;
                  }
               }
               else
               {
                  invokeOperation(wrapper.getInstance(), Operation.handle, message);
                  messageDispatchSuccessful = true;
               }
            }
            finally { wrapper.releaseLock(); }
         } 
         else  // ... we didn't get the lock
         {
            if (logger.isTraceEnabled())
               logger.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
            statCollector.messageDiscarded(message);
            statCollector.messageCollision(message);
         }
      } while (evictedAndBlocking);

      return messageDispatchSuccessful;
   }

   /**
    *  Returns the instance associated with the given message, creating it if
    *  necessary. Will append the message to the instance's work queue (which
    *  may also contain an activation invocation).
    */
   protected InstanceWrapper getInstanceForDispatch(Object message) throws ContainerException
   {
      if (message == null)
         throw new ContainerException("the container for " + clusterId + " attempted to dispatch null message.");

      if (!prototype.isMessageSupported(message))
         throw new ContainerException("the container for " + clusterId + " has a prototype " + SafeString.valueOf(prototype) + 
               " that does not handle messages of class " + SafeString.valueOfClass(message));

      Object key = getKeyFromMessage(message);
      InstanceWrapper wrapper = getInstanceForKey(key);
      return wrapper;
   }

   public void evict()
   {
      doEvict(new EvictCheck()
      {
         @Override
         public boolean shouldEvict(Object key, InstanceWrapper wrapper)
         {
            Object instance = wrapper.getInstance();
            try 
            {
               return prototype.invokeEvictable(instance);
            } 
            catch (InvocationTargetException e)
            {
               logger.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) + 
                     " resulted in an exception.",e.getCause());
            }
            catch (IllegalAccessException e)
            {
               logger.warn("It appears that the method for checking the eviction or passivating the Mp " + SafeString.objectDescription(instance) + 
                     " is not defined correctly. Is it visible?",e);
            }
            
            return false;
         }
         
         @Override
         public boolean isGenerallyEvitable() { return prototype.isEvictableSupported(); }
         @Override
         public boolean shouldStopEvicting() { return false; }
      });
   }
   
   private interface EvictCheck
   {
      boolean isGenerallyEvitable();
      
      boolean shouldEvict(Object key, InstanceWrapper wrapper);
      
      boolean shouldStopEvicting();
   }
   
   private void doEvict(EvictCheck check)
   {
      if (!check.isGenerallyEvitable() || !isRunning)
         return;

      StatsCollector.TimerContext tctx = null;
      try
      {
         tctx = statCollector.evictionPassStarted();

         // we need to make a copy of the instances in order to make sure
         // the eviction check is done at once.
         Map<Object,InstanceWrapper> instancesToEvict = new HashMap<Object,InstanceWrapper>(instances.size() + 10);
         instancesToEvict.putAll(instances);

         while (instancesToEvict.size() > 0 && instances.size() > 0 && isRunning && !check.shouldStopEvicting())
         {
            // store off anything that passes for later removal. This is to avoid a
            // ConcurrentModificationException.
            Set<Object> keysToRemove = new HashSet<Object>();

            for (Map.Entry<Object, InstanceWrapper> entry : instancesToEvict.entrySet())
            {
               if (check.shouldStopEvicting())
                  break;
               
               Object key = entry.getKey();
               InstanceWrapper wrapper = entry.getValue();
               boolean gotLock = false;
               try {
                  gotLock = wrapper.tryLock();
                  if (gotLock) {

                     // since we got here we're done with this instance,
                     // so add it's key to the list of keys we plan don't
                     // need to return to.
                     keysToRemove.add(key);
                     
                     boolean removeInstance = false;

                     try 
                     {
                        if (check.shouldEvict(key,wrapper))
                        {
                           removeInstance = true;
                           wrapper.markEvicted();
                           prototype.passivate(wrapper.getInstance());
//                           wrapper.markPassivated();
                        }
                     } 
                     catch (Throwable e)
                     {
                        Object instance = null;
                        try { instance = wrapper.getInstance(); } catch(Throwable th) {} // not sure why this would ever happen
                        logger.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance == null ? wrapper : instance) + 
                              " resulted in an exception.",e);
                     }
                     
                     // even if passivate throws an exception, if the eviction check returned 'true' then
                     //  we need to remove the instance.
                     if (removeInstance)
                     {
                        instances.remove(key);
                        statCollector.messageProcessorDeleted(key);
                     }
                  }
               } finally {
                  if (gotLock)
                     wrapper.releaseLock();
               }

            }

            // now clean up everything we managed to get hold of
            for (Object key : keysToRemove)
               instancesToEvict.remove(key);

         }
      }
      finally
      {
         if (tctx != null) tctx.stop();
      }
   }

   public void startEvictionThread(long evictionFrequency, TimeUnit timeUnit) {
      if (0 == evictionFrequency || null == timeUnit) {
         logger.warn("Eviction Thread cannot start with zero frequency or null TimeUnit {} {}", evictionFrequency, timeUnit);
         return;
      }

      if (prototype != null && prototype.isEvictableSupported()) {
         evictionScheduler = Executors.newSingleThreadScheduledExecutor();
         evictionScheduler.scheduleWithFixedDelay(new Runnable(){ public void run(){ evict(); }}, evictionFrequency, evictionFrequency, timeUnit);
      }
   }

   private ExecutorService outputExecutorService = null;
   private int outputConcurrency = -1;
   private Object lockForExecutorServiceSetter = new Object();

   @Override
   public void setConcurrency(int concurrency)
   {
      synchronized(lockForExecutorServiceSetter)
      {
         outputConcurrency = concurrency;
         if (prototype != null) // otherwise this isn't initialized yet
            setupOutputConcurrency();
      }
   }
   
   private void setupOutputConcurrency()
   {
      if (prototype.isOutputSupported() && isRunning)
      {
         synchronized(lockForExecutorServiceSetter)
         {
            if (outputConcurrency > 1)
               outputExecutorService = Executors.newFixedThreadPool(outputConcurrency);
            else
            {
               if (outputExecutorService != null)
                  outputExecutorService.shutdown();
               outputExecutorService = null;
            }
         }
      }
   }

   // This method MUST NOT THROW
   public void outputPass() {
      if (!prototype.isOutputSupported() || !isRunning)
         return;

      // take a snapshot of the current container state.
      LinkedList<InstanceWrapper> toOutput = new LinkedList<InstanceWrapper>(instances.values());

      Executor executorService = null;
      Semaphore taskLock = null;
      synchronized(lockForExecutorServiceSetter)
      {
         executorService = outputExecutorService;
         if (executorService != null)
            taskLock = new Semaphore(outputConcurrency);
      }

      // This keeps track of the number of concurrently running
      // output tasks so that this method can wait until they're
      // all done to return.
      //
      // It's also used as a condition variable signaling on its
      // own state changes.
      final AtomicLong numExecutingOutputs = new AtomicLong(0);

      // keep going until all of the outputs have been invoked
      while (toOutput.size() > 0 && isRunning)
      {
         for (final Iterator<InstanceWrapper> iter = toOutput.iterator(); iter.hasNext();) 
         {
            final InstanceWrapper wrapper = iter.next();
            boolean gotLock = false;

            gotLock = wrapper.tryLock();

            if (gotLock) 
            {
               // If we've been evicted then we're on our way out
               // so don't do anything else with this.
               if (wrapper.isEvicted())
               {
                  iter.remove();
                  wrapper.releaseLock();
                  continue;
               } 

               final Object instance = wrapper.getInstance(); // only called while holding the lock
               final Semaphore taskSepaphore = taskLock;

               // This task will release the wrapper's lock.
               Runnable task = new Runnable()
               {
                  @Override
                  public void run()
                  {
                     try
                     {
                        if (isRunning && !wrapper.isEvicted()) 
                           invokeOperation(instance, Operation.output, null); 
                     }
                     finally 
                     {
                        wrapper.releaseLock();

                        // this signals that we're done.
                        synchronized(numExecutingOutputs)
                        {
                           numExecutingOutputs.decrementAndGet();
                           numExecutingOutputs.notifyAll();
                        }
                        if (taskSepaphore != null) taskSepaphore.release(); 
                     }
                  }
               };

               synchronized(numExecutingOutputs)
               {
                  numExecutingOutputs.incrementAndGet();
               }

               if (executorService != null)
               {
                  try
                  {
                     taskSepaphore.acquire();
                     executorService.execute(task);
                  }
                  catch (RejectedExecutionException e)
                  {
                     // this may happen because of a race condition between the 
                     taskSepaphore.release();
                     wrapper.releaseLock(); // we never got into the run so we need to release the lock
                  }
                  catch (InterruptedException e)
                  {
                     // this can happen while blocked in the semaphore.acquire.
                     // if we're no longer running we should just get out
                     // of here.
                     //
                     // Not releasing the taskSepaphore assumes the acquire never executed.
                     // if (since) the acquire never executed we also need to release the
                     //  wrapper lock or that Mp will never be usable again.
                     wrapper.releaseLock(); // we never got into the run so we need to release the lock
                  }
               }
               else
                  task.run();

               iter.remove();
            } // end if we got the lock
         } // end loop over every Mp
      } // end while there are still Mps that haven't had output invoked.

      // =======================================================
      // now make sure all of the running tasks have completed
      synchronized(numExecutingOutputs)
      {
         while (numExecutingOutputs.get() > 0)
         {
            try { numExecutingOutputs.wait(); }
            catch (InterruptedException e)
            {
               // if we were interupted for a shutdown then just stop
               // waiting for all of the threads to finish
               if (!isRunning)
                  break;
               // otherwise continue checking.
            }
         }
      }
      // =======================================================
   }

   @Override
   public void invokeOutput()
   {
      StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted();
      try { outputPass(); }
      finally { tctx.stop(); }
   }

   //----------------------------------------------------------------------------
   // Internals
   //----------------------------------------------------------------------------

   private Object getKeyFromMessage(Object message) throws ContainerException
   {
      Object key = null;
      try
      {
         key = keyMethods.invokeGetter(message);
      }
      catch (IllegalArgumentException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               ". Are you sure that the method to retrieve the key takes no parameters?",e);
      }
      catch(IllegalAccessException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               ". Are you sure that the method to retrieve the key is publically accessible (both the class and the method must be public)?");
      }
      catch(InvocationTargetException e)
      {
         throw new ContainerException("the container for " + clusterId + " is unable to retrieve key from message " + SafeString.objectDescription(message) +
               " because the method to retrieve the key threw an exception.",e.getCause());
      }

      if (key == null)
         throw new ContainerException("the container for " + clusterId + " retrieved a null message key from " + 
               SafeString.objectDescription(message));
      return key;
   }

   ConcurrentHashMap<Object, Boolean> keysBeingWorked = new ConcurrentHashMap<Object, Boolean>();
   
   /**
    * This is required to return non null or throw a ContainerException
    * @throws IllegalAccessException 
    * @throws InvocationTargetException 
    */
   public InstanceWrapper getInstanceForKey(Object key) throws ContainerException
   {
      // common case has "no" contention
      InstanceWrapper wrapper = instances.get(key);
      if(wrapper != null)
         return wrapper;
      
      // otherwise we will be working to get one.
      Boolean tmplock = new Boolean(true);
      Boolean lock = keysBeingWorked.putIfAbsent(key, tmplock);
      if (lock == null)
         lock = tmplock;

      // otherwise we'll do an atomic check-and-update
      synchronized (lock)
      {
         wrapper = instances.get(key); // double checked lock?????
         if (wrapper != null)
            return wrapper;

         Object instance = null;
         try
         {
            instance = prototype.newInstance();
         }
         catch(InvocationTargetException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone method threw an exception.",e.getCause());
         }
         catch(IllegalAccessException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone method is not accessible. Is the class public? Is the clone method public? Does the class implement Cloneable?",e);
         }
         catch(RuntimeException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) + 
                  " because the clone invocation resulted in an unknown exception.",e);
         }

         if (instance == null)
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " + 
                  SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                  ". The value returned from the clone call appears to be null.");

         // activate
         byte[] data = null;
         if (logger.isTraceEnabled())
            logger.trace("the container for " + clusterId + " is activating instance " + String.valueOf(instance)
                  + " with " + ((data != null) ? data.length : 0) + " bytes of data"
                  + " via " + SafeString.valueOf(prototype));

         try
         {
            prototype.activate(instance, key, data);
         }
         catch(IllegalArgumentException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  ". Is it declared to take a byte[]?",e);
         }
         catch(IllegalAccessException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  ". Is the active method accessible - the class is public and the method is public?",e);
         }
         catch(InvocationTargetException e)
         {
            // If the activate can throw this exception explicitly then we want to note it on the
            // ContainerException so that we can log appropriately at the outer level.
            Throwable cause = e.getCause();
            ContainerException toThrow = 
                  new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                        " because the method itself threw an exception.",cause);
            toThrow.setExpected(prototype.activateCanThrowChecked(cause));
            throw toThrow;
         }
         catch(RuntimeException e)
         {
            throw new ContainerException("the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +  
                  " because of an unknown exception.",e);
         }

         // we only want to create a wrapper and place the instance into the container
         //  if the instance activated correctly. If we got here then the above try block
         //  must have been successful.
         wrapper = new InstanceWrapper(instance); // null check above.
         instances.put(key, wrapper); // once it goes into the map, we can remove it from the 'being worked' set
         keysBeingWorked.remove(key); // remove it from the keysBeingWorked since any subsequent call will get
                                      //  the newly added one.
         statCollector.messageProcessorCreated(key);

         return wrapper;
      }
   }

   public enum Operation { handle, output };

   /**
    * helper method to invoke an operation (handle a message or run output) handling all of hte
    * exceptions and forwarding any results.
    */
   private void invokeOperation(Object instance, Operation op, Object message)
   {
      if (instance != null) // possibly passivated ...
      {
         try
         {
            statCollector.messageDispatched(message);
            Object result = op == Operation.output ? prototype.invokeOutput(instance) : prototype.invoke(instance, message,statCollector);
            statCollector.messageProcessed(message);
            if (result != null)
            {
               dispatcher.dispatch(result);
            }
         }
         catch(ContainerException e)
         {
            logger.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " + 
                  SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + SafeString.objectDescription(message)) : ""),e);
            statCollector.messageFailed(false);
         }
         // this is an exception thrown as a result of the reflected call having an illegal argument.
         // This should actually be impossible since the container itself manages the calling.
         catch(IllegalArgumentException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?", e);
            statCollector.messageFailed(true);
         }
         // can't access the method? Did the app developer annotate it correctly?
         catch(IllegalAccessException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due an access problem. Is the method public?", e);
            statCollector.messageFailed(true);
         }
         // The app threw an exception.
         catch(InvocationTargetException e)
         {
            logger.warn("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " because an exception was thrown by the Message Processeor itself.", e.getCause() );
            statCollector.messageFailed(true);
         }
         // RuntimeExceptions bookeeping
         catch (RuntimeException e)
         {
            logger.error("the container for " + clusterId + " failed when trying to invoke " + prototype.invokeDescription(op,message) + 
                  " due to an unknown exception.", e);
            statCollector.messageFailed(false);

            if (op == Operation.handle)
               throw e;
         }
      }
   }

}
