package com.nokia.dempsy.guice.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Transport;
import com.nokia.dempsy.messagetransport.tcp.TcpTransport;
import com.nokia.dempsy.monitoring.StatsCollectorFactory;
import com.nokia.dempsy.monitoring.coda.StatsCollectorFactoryCoda;
import com.nokia.dempsy.mpcluster.MpClusterSessionFactory;
import com.nokia.dempsy.mpcluster.zookeeper.ZookeeperSessionFactory;
import com.nokia.dempsy.router.ClusterInformation;
import com.nokia.dempsy.router.CurrentClusterCheck;
import com.nokia.dempsy.router.DefaultRoutingStrategy;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.router.SlotInformation;
import com.nokia.dempsy.router.SpecificClusterCheck;
import com.nokia.dempsy.serialization.Serializer;
import com.nokia.dempsy.serialization.java.JavaSerializer;

public class DempsyModule extends AbstractModule
{
   
   public static final String applicationKey = "application";
   public static final String clusterKey = "cluster";
   
   public void configure()
   {
      // set the StatsCollectorFactory implementation
      bind(StatsCollectorFactory.class).to(StatsCollectorFactoryCoda.class);
      
      // set the Serializer implementation
      bind(new TypeLiteral<Serializer<Object>>() {}).toInstance(new JavaSerializer<Object>());
      
      // set the Transport implementation
      bind(Transport.class).to(TcpTransport.class);
   }
   
   @Provides
   public CurrentClusterCheck provideClusterCheck() throws DempsyException
   {
      return new SpecificClusterCheck(
            new ClusterId(
                  getRequiredParam(applicationKey, "application name"),
                  getRequiredParam(clusterKey, "cluster name")));
   }
   
   @Provides
   public RoutingStrategy provideRoutingStrategy() throws DempsyException
   {
      return new DefaultRoutingStrategy(
            Integer.valueOf(getRequiredParam("total_slots_count", "total slots to allocate for cluster")),
            Integer.valueOf(getRequiredParam("min_node_count", "minimum nodes required")));
   }
   
   @Provides
   public MpClusterSessionFactory<ClusterInformation,SlotInformation> provideClusterSessionFactory() throws DempsyException
   {
      return new ZookeeperSessionFactory<ClusterInformation,SlotInformation>(
            getRequiredParam("zk_connect", "connect string for zookeeper"), 
            Integer.valueOf(getRequiredParam("zk_session_timeout","zookeeper session timeout")));
   }
   
   private String getRequiredParam(String key, String keyDescriptionForErrorMessage) throws DempsyException
   {
      String ret = System.getProperty(key);
      if (ret == null)
         throw new DempsyException("You need to supply \"-D" + key + 
               "=[" + keyDescriptionForErrorMessage + "]\" on the command line.");
      return ret;
   }

}
