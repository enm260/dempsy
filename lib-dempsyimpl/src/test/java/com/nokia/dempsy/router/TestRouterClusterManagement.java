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

package com.nokia.dempsy.router;

import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageKey;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.invm.LocalClusterSessionFactory;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.monitoring.basic.BasicStatsCollectorFactory;
import com.nokia.dempsy.router.RoutingStrategy.Inbound;
import com.nokia.dempsy.router.microshard.MicroShardUtils;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class TestRouterClusterManagement
{
   Router routerFactory = null;
   RoutingStrategy.Inbound inbound = null;
   
   public static class GoodMessage
   {
      @MessageKey
      public String key() { return "hello"; }
   }
   
   public static class GoodMessageChild extends GoodMessage
   {
   }
   
   
   @MessageProcessor
   public static class GoodTestMp
   {
      @MessageHandler
      public void handle(GoodMessage message) {}
   }
   
   public static class Message
   {
      @MessageKey
      public String key() { return "hello"; }
   }
   
   @Before
   public void init() throws Throwable
   {
      final ClusterId clusterId = new ClusterId("test", "test-slot");
      Destination destination = new Destination() {};
      ApplicationDefinition app = new ApplicationDefinition(clusterId.getApplicationName());
      DecentralizedRoutingStrategy strategy = new DecentralizedRoutingStrategy(1, 1);
      app.setRoutingStrategy(strategy);
      app.setSerializer(new JavaSerializer<Object>());
      ClusterDefinition cd = new ClusterDefinition(clusterId.getMpClusterName());
      cd.setMessageProcessorPrototype(new GoodTestMp());
      app.add(cd);
      app.initialize();
      
      LocalClusterSessionFactory mpfactory = new LocalClusterSessionFactory();
      ClusterInfoSession session = mpfactory.createSession();
      
      MicroShardUtils utils = new MicroShardUtils(clusterId);
      utils.mkClusterDir(session, null);

      // fake the inbound side setup
      inbound = strategy.createInbound(session,clusterId, 
            new Dempsy(){ public List<Class<?>> gm(ClusterDefinition clusterDef) { return super.getAcceptedMessages(clusterDef); }}.gm(cd), 
         destination,new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener()
         {
            @Override
            public void keyspaceResponsibilityChanged(boolean less, boolean more) { }

            @Override
            public void setInboundStrategy(Inbound inbound) { }
         });
      
      routerFactory = new Router(cd);
      routerFactory.setClusterSession(session);
      routerFactory.setStatsCollector(new BasicStatsCollectorFactory().createStatsCollector(clusterId, new Destination() {} ));
      routerFactory.start();
   }
   
   @After
   public void stop() throws Throwable
   {
      routerFactory.stop();
      inbound.stop();
   }
   
   @Test
   public void testGetRouterNotFound()
   {
      // lazy load requires the using of the router.
      routerFactory.dispatch(new Message());
      Collection<RoutingStrategy.Outbound> router = routerFactory.outboundManager.retrieveOutbounds(Message.class);
      Assert.assertNotNull(router);
      Assert.assertEquals(0,router.size());
      Assert.assertTrue(((DecentralizedRoutingStrategy.OutboundManager)(routerFactory.outboundManager)).getTypesWithNoOutbounds().contains(Message.class));
   }
   
   @Test
   public void testGetRouterFound()
   {
      routerFactory.dispatch(new GoodMessage());
      Collection<RoutingStrategy.Outbound> routers = routerFactory.outboundManager.retrieveOutbounds(GoodMessage.class);
      Assert.assertNotNull(routers);
      Assert.assertEquals(false, 
            ((DecentralizedRoutingStrategy.OutboundManager)(routerFactory.outboundManager)).getTypesWithNoOutbounds().contains(GoodMessage.class));
      Collection<RoutingStrategy.Outbound> routers1 = routerFactory.outboundManager.retrieveOutbounds(GoodMessageChild.class);
      Assert.assertEquals(routers, routers1);
      Assert.assertEquals(new ClusterId("test", "test-slot"), routerFactory.getThisClusterId());
   }
   
   @Test
   public void testChangingClusterInfo() throws Throwable
   {
      // check that the message didn't go through.
      System.setProperty("nodecount", "1");
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
            "testDempsy/Dempsy.xml", "testDempsy/ClusterInfo-LocalActx.xml", "testDempsy/Serializer-KryoActx.xml",
            "testDempsy/RoutingStrategy-DecentralizedActx.xml", "testDempsy/Transport-PassthroughActx.xml", "testDempsy/SimpleMultistageApplicationActx.xml" );
      Dempsy dempsy = (Dempsy)context.getBean("dempsy");
      ClusterInfoSessionFactory factory = dempsy.getClusterSessionFactory();
      ClusterInfoSession session = factory.createSession();
      ClusterId curCluster = new ClusterId("test-app", "test-cluster1");
      new MicroShardUtils(curCluster).mkClusterDir(session, new DecentralizedRoutingStrategy.DefaultRouterClusterInfo(20,2,null));
      session.stop();
      dempsy.stop();
   }
}
