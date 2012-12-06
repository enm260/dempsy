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

package com.nokia.dempsy.router.microshard;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.router.RoutingStrategy.Inbound;

public class MicroShardRoutingStrategy extends DecentralizedRoutingStrategy
{
   private static final int resetDelay = 500;
   private Logger logger = LoggerFactory.getLogger(MicroShardRoutingStrategy.class);
   
   public MicroShardRoutingStrategy(int defaultTotalShards, int defaultNumNodes)
   {
      super(defaultTotalShards,defaultNumNodes);
   }
   
   public static class MicroShardInfo extends DefaultRouterShardInfo
   {
      private String nodePath;
      
      public MicroShardInfo(Destination destination, String nodePath, int shardIndex, int numShards)
      {
         setShardIndex(shardIndex);
         setTotalAddress(numShards);
         setDestination(destination);
         this.nodePath = nodePath;
      }
      
      public String getNodePath() { return nodePath; }
      public void setNodePath(String nodePath) { this.nodePath = nodePath; }
   }

   public class MSInbound implements RoutingStrategy.Inbound, ClusterInfoWatcher
   {
      private ClusterInfoSession session;
      private Destination thisDestination;
      private ClusterId clusterId;
      private KeyspaceResponsibilityChangeListener listener;
      private MicroShardInfo thisInfo = null;
      private CopyOnWriteArraySet<Integer> ownShards = new CopyOnWriteArraySet<Integer>();
      private MicroShardUtils msutils;
      private String myNodePath = null;
      private DefaultRouterClusterInfo clusterInfo;
      private AtomicBoolean isInited = new AtomicBoolean(false);
      private AtomicBoolean isRunning = new AtomicBoolean(true);
      
      public MSInbound(ClusterInfoSession cluster, ClusterId clusterId, 
            Collection<Class<?>> messageTypes, Destination thisDestination,
            KeyspaceResponsibilityChangeListener listener)
      {
         this.listener = listener;
         this.session = cluster;
         this.thisDestination = thisDestination;
         this.clusterId = clusterId;
         this.msutils = new MicroShardUtils(this.clusterId);
         this.clusterInfo = new DefaultRouterClusterInfo(defaultTotalShards,defaultNumNodes,messageTypes);
         process();
      }

      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return ownShards.contains(Math.abs(messageKey.hashCode()%clusterInfo.getTotalShardCount()));
      }

      @Override
      public synchronized void stop()
      {
         try { if (myNodePath != null) session.rmdir(myNodePath); }
         catch(ClusterInfoException e)
         {
            logger.error("Error removing directory " + myNodePath, e);
         }
         
         isRunning.set(false);
      }
      
      @Override
      public boolean isInitialized() { return isInited.get(); }
      
      ScheduledFuture<?> currentlyWaitingOn = null;

      @Override
      public synchronized void process()
      {
         boolean completed = false;
         
         try
         {
            // ok ... we're going to execute this now. So if we have an outstanding scheduled task we
            // need to cancel it.
            if (currentlyWaitingOn != null)
            {
               currentlyWaitingOn.cancel(false);
               currentlyWaitingOn = null;
            }
            
            if (logger.isTraceEnabled())
               logger.trace("Resetting Inbound Strategy for cluster " + clusterId);
            
            //===============================================
            // get the cluster info and make sure it matches what we think it should be
            try
            {
               DefaultRouterClusterInfo ci = 
                     (DefaultRouterClusterInfo)session.getData(msutils.getClusterDir(), null); // we're going to be optimistic about consistency
                                                                                               //  and not look for updates from other nodes.
               
               // if we got here we need to check to make sure clusterInfo corresponds to the
               //   what's registered in the ClusterInfoSession
               if (ci == null) // then there is a race. So let's compete.
               {
                  msutils.mkAllPersistentAppDirs(session, clusterInfo);
                  session.setData(msutils.getClusterDir(), clusterInfo);
               }
               else if (!clusterInfo.equals(ci))
               {
                  logger.error("There is a difference between the Cluster information we're configured with (" + SafeString.valueOf(clusterInfo) + 
                        ") and that registered with ZooKeeper " + SafeString.valueOf(ci));
                  logger.error("THIS IS A MAJOR PROBLEM. PLEASE RESTART THE ENTIRE APPLICATION WITH THE SAME CONFIGURATION.");
               }
            }
            catch (ClusterInfoException.NoNodeException e)
            {
               // this means we're the first one here.
               msutils.mkAllPersistentAppDirs(session, clusterInfo);
               session.setData(msutils.getClusterDir(), clusterInfo);
            }
            //===============================================
            
            //===============================================
            // if we don't have a node path yet, or we've been reset somehow, re-get the node bin
            try
            {
               if (myNodePath == null || !session.exists(myNodePath, null))
                  myNodePath = session.mkdir(msutils.getNodesDir() + "/node_", DirMode.EPHEMERAL_SEQUENTIAL);
            }
            catch (ClusterInfoException.NoNodeException e)
            {
               // this means we're the first one here.
               msutils.mkAllPersistentAppDirs(session, clusterInfo);
               if (myNodePath == null || !session.exists(myNodePath, null))
                  myNodePath = session.mkdir(msutils.getNodesDir() + "/node_", DirMode.EPHEMERAL_SEQUENTIAL);
            }
            
            if(myNodePath != null)
            {
               thisInfo = new MicroShardInfo(thisDestination,myNodePath,-1,defaultTotalShards);
               completed = session.exists(myNodePath, this);
            }
            //===============================================
         }
         catch (ClusterInfoException e)
         {
            logger.warn("Problem trying to initialize the inbound microsharding strategy." + 
                  " Will try again in " + resetDelay + " milliseconds.", e);
         }
         finally
         {
            if (!completed)
            {
               ScheduledExecutorService sched = getScheduledExecutor();
               if (sched != null)
                   currentlyWaitingOn = sched.schedule(new Runnable(){
                     @Override
                     public void run() { process(); }
                  }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
      }
   }
   
   @Override
   public RoutingStrategy.Inbound createInbound(ClusterInfoSession cluster, ClusterId clusterId,
         Collection<Class<?>> messageTypes, Destination thisDestination, Inbound.KeyspaceResponsibilityChangeListener listener)
   {
      return new MSInbound(cluster, clusterId, messageTypes, thisDestination,listener);
   }

}
