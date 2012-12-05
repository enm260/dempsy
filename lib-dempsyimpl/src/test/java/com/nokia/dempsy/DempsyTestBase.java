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

package com.nokia.dempsy;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.annotations.Output;
import com.nokia.dempsy.annotations.Start;
import com.nokia.dempsy.cluster.DisruptibleSession;
import com.nokia.dempsy.cluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.serialization.kryo.KryoOptimizer;

public class DempsyTestBase
{
   /**
    * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in 
    * every runAllCombinations call. This can make TestDempsy run for a loooooong time.
    */
   public static boolean hardcore = false;

   protected static Logger logger;
   protected static long baseTimeoutMillis = 20000; // 20 seconds

   public String[] dempsyConfigs = new String[] { "testDempsy/Dempsy.xml" };

   public String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/ClusterInfo-LocalActx.xml" };
   public String[][] transports = new String[][] {
         { "testDempsy/Transport-PassthroughActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml" }, 
         { "testDempsy/Transport-BlockingQueueActx.xml" }, 
         { "testDempsy/Transport-TcpActx.xml", "testDempsy/Transport-TcpFailSlowActx.xml", "testDempsy/Transport-TcpWithOverflowActx.xml", "testDempsy/Transport-TcpBatchedOutputActx.xml" }
   };

   public String[] serializers = new String[]
         { "testDempsy/Serializer-JavaActx.xml", "testDempsy/Serializer-KryoActx.xml", "testDempsy/Serializer-KryoOptimizedActx.xml" };

   // bad combinations.
   public List<ClusterId> badCombos = Arrays.asList(new ClusterId[] {
         // this is a hack ... use a ClusterId as a String tuple for comparison

         // the passthrough Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughActx.xml") , 
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-PassthroughBlockingActx.xml") , 

         // the blockingqueue Destination is not serializable but zookeeper requires it to be
         new ClusterId("testDempsy/ClusterInfo-ZookeeperActx.xml", "testDempsy/Transport-BlockingQueueActx.xml") 
   });

   public static InitZookeeperServerBean zkServer = null;

   @BeforeClass
   public static void setupZookeeperSystemVars() throws IOException
   {
      System.setProperty("application", "test-app");
      System.setProperty("cluster", "test-cluster2");
      zkServer = new InitZookeeperServerBean();
   }

   @AfterClass
   public static void shutdownZookeeper()
   {
      zkServer.stop();
   }

   @Before
   public void init()
   {
      KeySourceImpl.disruptSession = false;
      KeySourceImpl.infinite = false;
      KeySourceImpl.pause = new CountDownLatch(0);
      TestMp.currentOutputCount = 10;
      TestMp.activateCheckedException = false;
   }

   public static class TestMessage implements Serializable
   {
      private static final long serialVersionUID = 1L;
      private String val;

      @SuppressWarnings("unused") // required for Kryo
      private TestMessage() {} 

      public TestMessage(String val) { this.val = val; }

      @MessageKey
      public String get() { return val; } 

      public boolean equals(Object o) 
      {
         return o == null ? false :
            String.valueOf(val).equals(String.valueOf(((TestMessage)o).val)); 
      }
   }

   public static class TestKryoOptimizer implements KryoOptimizer
   {

      @Override
      public void preRegister(Kryo kryo)
      {
         kryo.setRegistrationRequired(true);
      }

      @Override
      public void postRegister(Kryo kryo)
      {
         @SuppressWarnings("unchecked")
         FieldSerializer<TestMessage> valSer = (FieldSerializer<TestMessage>)kryo.getSerializer(TestMessage.class);
         valSer.setFieldsCanBeNull(false);
      }

   }

   public static class ActivateCheckedException extends Exception
   {
      private static final long serialVersionUID = 1L;
      public ActivateCheckedException(String message) { super(message); }
   }
   
   @MessageProcessor
   public static class TestMp implements Cloneable
   {
      public static int currentOutputCount = 10;

      // need a mutable object reference
      public AtomicReference<TestMessage> lastReceived = new AtomicReference<TestMessage>();
      public AtomicLong outputCount = new AtomicLong(0);
      public CountDownLatch outputLatch = new CountDownLatch(currentOutputCount);
      public AtomicInteger startCalls = new AtomicInteger(0);
      public AtomicInteger cloneCalls = new AtomicInteger(0);
      public AtomicLong handleCalls = new AtomicLong(0);
      public AtomicReference<String> failActivation = new AtomicReference<String>();
      public AtomicBoolean haveWaitedOnce = new AtomicBoolean(false);
      public static boolean activateCheckedException = false;

      @Start
      public void start()
      {
         startCalls.incrementAndGet();
      }

      @MessageHandler
      public void handle(TestMessage message)
      {
         lastReceived.set(message);
         handleCalls.incrementAndGet();
      }

      @Activation
      public void setKey(String key) throws ActivateCheckedException
      {
         // we need to wait at least once because sometime pre-instantiation 
         // goes so fast the test fails because it fails to register on the statsCollector.
         if (!haveWaitedOnce.get())
         {
            try { Thread.sleep(3); } catch (Throwable th) {}
            haveWaitedOnce.set(true);
         }

         if (key.equals(failActivation.get()))
         {
            String message = "Failed Activation For " + key;
            if (activateCheckedException)
               throw new ActivateCheckedException(message);
            else
               throw new RuntimeException(message);
         }
      }

      @Override
      public TestMp clone() throws CloneNotSupportedException 
      {
         cloneCalls.incrementAndGet();
         return (TestMp) super.clone();
      }

      @Output
      public void output()
      {
         outputCount.incrementAndGet();
         outputLatch.countDown();
      }
   }

   public static class OverflowHandler implements com.nokia.dempsy.messagetransport.OverflowHandler
   {

      @Override
      public void overflow(byte[] messageBytes)
      {
         logger.debug("Overflow:" + messageBytes);
      }

   }

   public static class TestAdaptor implements Adaptor
   {
      Dispatcher dispatcher;
      public Object lastSent;
      public volatile static boolean throwExceptionOnSetDispatcher = false; 

      @Override
      public void setDispatcher(Dispatcher dispatcher)
      {
         this.dispatcher = dispatcher;
         if (throwExceptionOnSetDispatcher) throw new RuntimeException("Forced RuntimeException"); 
      }

      @Override
      public void start() { }

      @Override
      public void stop() { }

      public void pushMessage(Object message)
      {
         lastSent = message;
         dispatcher.dispatch(message);
      }
   }

   public static class KeySourceImpl implements KeySource<String>
   {
      private Dempsy dempsy = null;
      private ClusterId clusterId = null;
      public static volatile boolean disruptSession = false;
      public static volatile boolean infinite = false;
      public static volatile CountDownLatch pause = new CountDownLatch(0);
      public static volatile KSIterable lastCreated = null;

      public void setDempsy(Dempsy dempsy) { this.dempsy = dempsy; }

      public void setClusterId(ClusterId clusterId) { this.clusterId = clusterId; }

      public class KSIterable implements Iterable<String>
      {
         public volatile String lastKey = "";
         public CountDownLatch m_pause = pause;
         public volatile boolean m_infinite = infinite;

         {
            lastCreated = this;
         }

         @Override
         public Iterator<String> iterator()
         {
            return new Iterator<String>()
                  {
               long count = 0;

               @Override
               public boolean hasNext() { if (count >= 1) kickClusterInfoMgr(); return m_infinite ? true : (count < 2);  }

               @Override
               public String next() { try { m_pause.await(); } catch (InterruptedException ie) {} count++; return (lastKey = "test" + count);}

               @Override
               public void remove() { throw new UnsupportedOperationException(); }

               private void kickClusterInfoMgr() 
               {
                  if (!disruptSession)
                     return;
                  disruptSession = false; // one disruptSession
                  Dempsy.Application.Cluster c = dempsy.getCluster(clusterId);
                  Object session = TestUtils.getSession(c);
                  if (session instanceof DisruptibleSession)
                  {
                     DisruptibleSession dses = (DisruptibleSession)session;
                     dses.disrupt();
                  }
               }
                  };
         }

      }

      @Override
      public Iterable<String> getAllPossibleKeys()
      {
         // The array is proxied to create the ability to rip out the cluster manager
         // in the middle of iterating over the key source. This is to create the 
         // condition in which the key source is being iterated while the routing strategy
         // is attempting to get slots.
         return new KSIterable();
      }
   }

   public static Object getMp(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0); // currently there is one node per cluster.
      return node.clusterDefinition.getMessageProcessorPrototype();
   }

   public static Adaptor getAdaptor(Dempsy dempsy, String appName, String clusterName)
   {
      Dempsy.Application.Cluster cluster = dempsy.getCluster(new ClusterId(appName,clusterName));
      Dempsy.Application.Cluster.Node node = cluster.getNodes().get(0); // currently there is one node per cluster.
      return node.clusterDefinition.getAdaptor();
   }

   public static interface Checker
   {
      public void check(ApplicationContext context) throws Throwable;
   }

   public static interface MultiCheck
   {
      public void check(ApplicationContext[] contexts) throws Throwable;
   }

   private static class WaitForShutdown implements Runnable
   {

      public boolean shutdown = false;
      public Dempsy dempsy = null;
      public CountDownLatch waitForShutdownDoneLatch = new CountDownLatch(1);

      WaitForShutdown(Dempsy dempsy) { this.dempsy = dempsy; }

      @Override
      public void run()
      {
         try { dempsy.waitToBeStopped(); shutdown = true; } catch(InterruptedException e) {}
         waitForShutdownDoneLatch.countDown();
      }

   }

   static class AlternatingIterable implements Iterable<String>
   {
      boolean hardcore = false;
      List<String> strings = null;

      public AlternatingIterable(boolean hardcore, String[] strings)
      {
         this.hardcore = hardcore; 
         this.strings = Arrays.asList(strings);
      }

      @Override
      public Iterator<String> iterator()
      {
         return hardcore ? strings.iterator() : 
            new Iterator<String>()
            {
            boolean done = false;

            @Override
            public boolean hasNext() { return !done; }

            @Override
            public String next(){ done = true; return strings.get(runCount % strings.size()); }

            @Override
            public void remove() { throw new UnsupportedOperationException(); }
            };
      }

   }

   public void runAllCombinations(String applicationContext, Checker checker) throws Throwable
   {
      runAllCombinations(checker,applicationContext);
   }

   static int runCount = 0;
   public void runAllCombinations(Checker checker, String... applicationContexts) throws Throwable
   {
      for (String clusterManager : clusterManagers)
      {
         for (String[] alternatingTransports : transports)
         {
            // select one of the alternatingTransports
            for (String transport : new AlternatingIterable(hardcore,alternatingTransports))
            {
               for (String serializer : new AlternatingIterable(hardcore,serializers))
               {
                  // alternate the dempsy configs
                  for (String dempsyConfig : new AlternatingIterable(hardcore,dempsyConfigs))
                  {

                     if (! badCombos.contains(new ClusterId(clusterManager,transport)))
                     {
                        String pass = Arrays.asList(applicationContexts).toString() + " test: " + (checker == null ? "none" : checker) + " using " + 
                              dempsyConfig + "," + clusterManager + "," + serializer + "," + transport;
                        try
                        {
                           logger.debug("*****************************************************************");
                           logger.debug(pass);
                           logger.debug("*****************************************************************");

                           String[] ctx = new String[4 + applicationContexts.length];
                           ctx[0] = dempsyConfig; ctx[1] = clusterManager; ctx[2] = transport; ctx[3] = serializer;

                           int count = 3;
                           for (String appctx : applicationContexts)
                              ctx[count++] = "testDempsy/" + appctx;

                           logger.debug("Starting up the appliction context ...");
                           ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(ctx);
                           actx.registerShutdownHook();

                           Dempsy dempsy = (Dempsy)actx.getBean("dempsy");

                           assertTrue(pass,TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy));

                           WaitForShutdown waitingForShutdown = new WaitForShutdown(dempsy);
                           Thread waitingForShutdownThread = new Thread(waitingForShutdown,"Waiting For Shutdown");
                           waitingForShutdownThread.start();
                           Thread.yield();

                           logger.debug("Running test ...");
                           if (checker != null)
                              checker.check(actx);
                           logger.debug("Done with test, stopping the application context ...");

                           actx.stop();
                           actx.destroy();

                           assertTrue(waitingForShutdown.waitForShutdownDoneLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                           assertTrue(waitingForShutdown.shutdown);

                           logger.debug("Finished this pass.");
                        }
                        catch (AssertionError re)
                        {
                           logger.error("***************** FAILED ON: " + pass);
                           throw re;
                        }

                        runCount++;
                     }
                  }
               }
            }
         }
      }
   }

   public void runAllCombinationsMultiDempsy(MultiCheck checker, String[]... applicationContextsArray) throws Throwable
   {
      for (String clusterManager : clusterManagers)
      {
         for (String[] alternatingTransports : transports)
         {
            // select one of the alternatingTransports
            for (String transport : new AlternatingIterable(hardcore,alternatingTransports))
            {
               for (String serializer : new AlternatingIterable(hardcore,serializers))
               {
                  // alternate the dempsy configs
                  for (String dempsyConfig : new AlternatingIterable(hardcore,dempsyConfigs))
                  {
                     if (! badCombos.contains(new ClusterId(clusterManager,transport)))
                     {
                        String pass = Arrays.asList(applicationContextsArray).toString() + " test: " + (checker == null ? "none" : checker) + " using " + 
                              dempsyConfig + "," + clusterManager + "," + serializer + "," + transport;
                        try
                        {
                           logger.debug("*****************************************************************");
                           logger.debug(pass);
                           logger.debug("*****************************************************************");

                           ClassPathXmlApplicationContext[] contexts = new ClassPathXmlApplicationContext[applicationContextsArray.length];
                           WaitForShutdown[] shutdownWaits = new WaitForShutdown[applicationContextsArray.length];
                           Dempsy[] dempsys = new Dempsy[applicationContextsArray.length];

                           // instantiate each Dempsy
                           int dempsyCount = 0;
                           for (String[] applicationContexts : applicationContextsArray)
                           {
                              String[] ctx = new String[4 + applicationContexts.length];
                              ctx[0] = dempsyConfig; ctx[1] = clusterManager; ctx[2] = transport; ctx[3] = serializer;

                              int count = 3;
                              for (String appctx : applicationContexts)
                                 ctx[count++] = "testDempsy/" + appctx;

                              logger.debug("Starting up the appliction context ...");
                              ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(ctx);
                              actx.registerShutdownHook();
                              contexts[dempsyCount] = actx;
                              Dempsy dempsy = (Dempsy)actx.getBean("dempsy");
                              dempsys[dempsyCount] = dempsy;
                              dempsyCount++;
                           }

                           dempsyCount = 0;
                           for (Dempsy dempsy : dempsys)
                           {
                              assertTrue(pass,TestUtils.waitForClustersToBeInitialized(baseTimeoutMillis, dempsy));

                              WaitForShutdown waitingForShutdown = new WaitForShutdown(dempsy);
                              Thread waitingForShutdownThread = new Thread(waitingForShutdown,"Waiting For Shutdown");
                              waitingForShutdownThread.start();
                              shutdownWaits[dempsyCount] = waitingForShutdown;
                              dempsyCount++;
                           }

                           logger.debug("Running test ...");
                           if (checker != null)
                              checker.check(contexts);
                           logger.debug("Done with test, stopping the application context ...");

                           for (ClassPathXmlApplicationContext actx : contexts)
                           {
                              actx.stop();
                              actx.destroy();
                           }

                           for (WaitForShutdown waitingForShutdown : shutdownWaits)
                           {
                              assertTrue(waitingForShutdown.waitForShutdownDoneLatch.await(baseTimeoutMillis, TimeUnit.MILLISECONDS));
                              assertTrue(waitingForShutdown.shutdown);
                           }

                           logger.debug("Finished this pass.");
                        }
                        catch (AssertionError re)
                        {
                           logger.error("***************** FAILED ON: " + pass);
                           throw re;
                        }

                        runCount++;
                     }
                  }
               }
            }
         }
      }
   }
}

