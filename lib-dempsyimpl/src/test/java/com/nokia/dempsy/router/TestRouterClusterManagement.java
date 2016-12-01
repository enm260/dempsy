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

import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.TestUtils;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.config.ApplicationDefinition;
import com.nokia.dempsy.config.ClusterDefinition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.Router.ClusterRouter;
import com.nokia.dempsy.router.RoutingStrategy.Inbound;
import com.nokia.dempsy.serialization.java.JavaSerializer;

import junit.framework.Assert;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.cluster.ClusterInfoSessionFactory;
import net.dempsy.cluster.local.LocalClusterSessionFactory;

public class TestRouterClusterManagement {
    Router routerFactory = null;
    RoutingStrategy.Inbound inbound = null;

    @MessageProcessor
    public static class GoodTestMp {
        @MessageHandler
        public void handle(final Exception message) {}
    }

    public String onodes = null;
    public String oslots = null;

    @Before
    public void init() throws Throwable {
        onodes = System.setProperty("min_nodes_for_cluster", "1");
        oslots = System.setProperty("total_slots_for_cluster", "20");

        final ClusterId clusterId = new ClusterId("test", "test-slot");
        final Destination destination = new Destination() {};
        final ApplicationDefinition app = new ApplicationDefinition(clusterId.getApplicationName());
        final DecentralizedRoutingStrategy strategy = new DecentralizedRoutingStrategy(1, 1);
        app.setRoutingStrategy(strategy);
        app.setSerializer(new JavaSerializer<Object>());
        final ClusterDefinition cd = new ClusterDefinition(clusterId.getMpClusterName());
        cd.setMessageProcessorPrototype(new GoodTestMp());
        app.add(cd);
        app.initialize();

        final LocalClusterSessionFactory mpfactory = new LocalClusterSessionFactory();
        final ClusterInfoSession session = mpfactory.createSession();

        TestUtils.createClusterLevel(clusterId, session);

        // fake the inbound side setup
        inbound = strategy.createInbound(session, clusterId,
                new Dempsy() {
                    public List<Class<?>> gm(final ClusterDefinition clusterDef) {
                        return super.getAcceptedMessages(clusterDef);
                    }
                }.gm(cd),
                destination, new RoutingStrategy.Inbound.KeyspaceResponsibilityChangeListener() {

                    @Override
                    public void keyspaceResponsibilityChanged(final Inbound inbound, final boolean less, final boolean more) {}
                });

        routerFactory = new Router(app);
        routerFactory.setClusterSession(session);
        routerFactory.setCurrentCluster(clusterId);
        routerFactory.initialize();
    }

    @After
    public void stop() throws Throwable {
        routerFactory.stop();
        inbound.stop();
        if (onodes != null)
            System.setProperty("min_nodes_for_cluster", onodes);
        if (oslots != null)
            System.setProperty("total_slots_for_cluster", oslots);
        onodes = oslots = null;
    }

    @Test
    public void testGetRouterNotFound() {
        final Set<ClusterRouter> router = routerFactory.getRouter(java.lang.String.class);
        Assert.assertNull(router);
        Assert.assertTrue(routerFactory.missingMsgTypes.containsKey(java.lang.String.class));
    }

    @Test
    public void testGetRouterFound() {
        final Set<ClusterRouter> routers = routerFactory.getRouter(java.lang.Exception.class);
        Assert.assertNotNull(routers);
        Assert.assertEquals(false, routerFactory.missingMsgTypes.containsKey(java.lang.Exception.class));
        final Set<ClusterRouter> routers1 = routerFactory.getRouter(ClassNotFoundException.class);
        Assert.assertEquals(routers, routers1);
        Assert.assertEquals(new ClusterId("test", "test-slot"), routerFactory.getThisClusterId());
    }

    @Test
    public void testChangingClusterInfo() throws Throwable {
        // check that the message didn't go through.
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "testDempsy/Dempsy.xml", "testDempsy/ClusterInfo-LocalActx.xml", "testDempsy/Serializer-KryoActx.xml",
                "testDempsy/Transport-PassthroughActx.xml", "testDempsy/SimpleMultistageApplicationActx.xml");
        final Dempsy dempsy = (Dempsy) context.getBean("dempsy");
        final ClusterInfoSessionFactory factory = dempsy.getClusterSessionFactory();
        final ClusterInfoSession session = factory.createSession();
        final ClusterId curCluster = new ClusterId("test-app", "test-cluster1");
        TestUtils.createClusterLevel(curCluster, session);
        session.setData(curCluster.asPath(), new DecentralizedRoutingStrategy.DefaultRouterClusterInfo(20, 2));
        session.stop();
        dempsy.stop();
    }

}
