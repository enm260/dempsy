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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;

public class MicroShardManager implements ClusterInfoWatcher
{
   private static final int resetDelay = 500;

   private Logger logger = LoggerFactory.getLogger(MicroShardManager.class);
   private ClusterInfoSession session;
   private String myEntry;
   private volatile ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private AtomicBoolean leader = new AtomicBoolean(false);
   private ConcurrentHashMap<String, MicroShardAppManager> appManagers = new ConcurrentHashMap<String, MicroShardAppManager>();
   private MicroShardUtils util;
   
   public MicroShardManager(ClusterInfoSessionFactory sessionFactory) throws ClusterInfoException
   {
      this(sessionFactory.createSession());
   }

   public MicroShardManager(ClusterInfoSession session)
   {
      this.session = session;
      util = new MicroShardUtils(new ClusterId("junk","junk"));
      
      process();
   }
   
   @Override
   public synchronized void process()
   {
      boolean completed = false;
      try
      {
         //=============================================================================
         // Leader election
         //=============================================================================
         util.mkManagerDir(session);
         util.mkAppRootDir(session);
         
         // if we don't have a node path yet, or we've been reset somehow, re-get the node bin
         if (myEntry == null || !session.exists(util.getManagerDir() + "/" + myEntry, null))
            myEntry = session.mkdir(util.getManagerDir() + "/M_", DirMode.EPHEMERAL_SEQUENTIAL);
         
         if (myEntry != null)
         {
            // take only the last part.
            myEntry = new File(myEntry).getName();
            Collection<String> managers = session.getSubdirs(util.getManagerDir(),this);
            
            if (managers != null && managers.size() > 0)
            {
               SortedSet<String> allOfUs = new TreeSet<String>();
               allOfUs.addAll(managers);
               String leaderPath = allOfUs.first();
               
               // are we the leader?
               boolean identifiedAsLeader = myEntry.equals(leaderPath);
               boolean wasLeader = leader.get();
               leader.set(identifiedAsLeader);
               if (identifiedAsLeader && !wasLeader)
                  manageApps();
            }
         }
         //=============================================================================
         
         completed = true;
      }
      catch (ClusterInfoException e)
      {
         logger.warn("Problem trying to handle cluster information in response to an update." + 
               " Will try again in " + resetDelay + " milliseconds.", e);
      }
      finally
      {
         if (!completed && scheduler != null)
            scheduler.schedule(new Runnable(){
               @Override
               public void run() { process(); }
            }, resetDelay, TimeUnit.MILLISECONDS);
      }
   }
   
   public boolean isLeader() { return leader.get(); }
   
   public void stop()
   {
      ScheduledExecutorService tmp = scheduler;
      scheduler = null;
      if (tmp != null)
      {
         try
         {
            tmp.shutdown();
         }
         catch (Throwable th)
         {
            logger.error("Couldn't shut down scheduler.");
         }
      }
   }
   
   @Override
   public void finalize()
   {
      stop();
   }
   
   private void manageApps()
   {
      boolean completed = false;
      try
      {
         if (leader.get())
         {
            // make sure we're registered for new apps.
            Collection<String> apps = session.getSubdirs(util.getAppRootDir(), new ClusterInfoWatcher()
            {
               @Override
               public void process() { manageApps(); }
            });

            if (apps != null && apps.size() > 0)
            {
               for (String cur : apps)
               {
                  if (cur != "manager") // this hack should be fixed.
                  {
                     MicroShardAppManager potential =  new MicroShardAppManager(cur);
                     MicroShardAppManager mgr = appManagers.putIfAbsent(cur,potential);
                     if (mgr == null)
                        mgr = potential;
                     mgr.process();
                     completed = true;
                  }
               }
            }
         }
      }
      catch (ClusterInfoException e)
      {
         logger.warn("Problem trying to initialize the inbound microsharding strategy." + 
               " Will try again in " + resetDelay + " milliseconds.", e);
      }
      finally
      {
         if (!completed && scheduler != null)
            scheduler.schedule(new Runnable(){
               @Override
               public void run() { manageApps(); }
            }, resetDelay, TimeUnit.MILLISECONDS);
      }

   }

   private class MicroShardAppManager implements ClusterInfoWatcher
   {
      private String applicationId;
      private String appDir;
      private ConcurrentHashMap<String, MicroShardClusterManager> clusterManagers = new ConcurrentHashMap<String, MicroShardClusterManager>();

      public MicroShardAppManager(String applicationId)
      {
         this.applicationId = applicationId;
         MicroShardUtils utils = new MicroShardUtils(new ClusterId(applicationId,"junk"));
         appDir = utils.getAppDir();
      }
   
      @Override
      public void process()
      {
         boolean completed = false;
         try
         {
            session.mkdir(appDir, DirMode.PERSISTENT);
            
            Collection<String> clusters = session.getSubdirs(appDir, this);
            
            for (String cur : clusters)
            {
               MicroShardClusterManager potential =  new MicroShardClusterManager(cur);
               MicroShardClusterManager mgr = clusterManagers.putIfAbsent(cur,potential);
               if (mgr == null)
                  mgr = potential;
               mgr.processData();
               mgr.processChildren();
               completed = true;
            }
         }
         catch (ClusterInfoException e)
         {
            logger.warn("Problem trying to initialize the inbound microsharding strategy." + 
                  " Will try again in " + resetDelay + " milliseconds.", e);
         }
         finally
         {
            if (!completed && scheduler != null)
               scheduler.schedule(new Runnable(){
                  @Override
                  public void run() { process(); }
               }, resetDelay, TimeUnit.MILLISECONDS);
         }
      }
      
      private class MicroShardClusterManager
      {
         private MicroShardUtils util;
         private Set<String> nodeSubdirs = new HashSet<String>();
         private AtomicReference<DecentralizedRoutingStrategy.DefaultRouterClusterInfo> clusterInfo = new AtomicReference<DecentralizedRoutingStrategy.DefaultRouterClusterInfo>();
         private ClusterId clusterId;
         private ClusterInfoWatcher childCallback = new ClusterInfoWatcher()
         {
            @Override
            public void process() { processChildren(); }
         };
         
         private ClusterInfoWatcher clusterDataCallback = new ClusterInfoWatcher()
         {
            @Override
            public void process() { processData(); }
         };

         public MicroShardClusterManager(String clusterName)
         {
            util = new MicroShardUtils(clusterId = new ClusterId(applicationId,clusterName));
         }
         
         public synchronized void processData()
         {
            boolean completed = false;
            
            try
            {
               initClusterInfoDefaultDirs();

               DecentralizedRoutingStrategy.DefaultRouterClusterInfo ci = (DecentralizedRoutingStrategy.DefaultRouterClusterInfo)session.getData(util.getClusterDir(), clusterDataCallback);
               if (ci != null)
                  clusterInfo.set(ci);
               completed = clusterInfo.get() != null;
            }
            catch (ClusterInfoException e)
            {
               logger.warn("Problem trying to initialize the microsharding manager for cluster " +
                     clusterId + ". Will try again in " + resetDelay + " milliseconds.", e);
            }
            finally
            {
               if (!completed && scheduler != null)
                  scheduler.schedule(new Runnable(){
                     @Override
                     public void run() { processData(); }
                  }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
         
         public synchronized void processChildren()
         {
            boolean completed = false;
            
            try
            {
               initClusterInfoDefaultDirs();
               
               //===========================================
               // sync the cluster info manager data with the 
               Collection<String> curNodeSubdirs = session.getSubdirs(util.getNodesDir(), childCallback);
               Set<String> dirsToAdd = missingFrom(curNodeSubdirs,nodeSubdirs);
               Set<String> dirsToRemove = missingFrom(nodeSubdirs,curNodeSubdirs);
               completed = rebalance(dirsToAdd, dirsToRemove);
               //===========================================
            }
            catch (ClusterInfoException e)
            {
               logger.warn("Problem trying to initialize the microsharding manager for cluster " +
                     clusterId + ". Will try again in " + resetDelay + " milliseconds.", e);
            }
            finally
            {
               if (!completed && scheduler != null)
                  scheduler.schedule(new Runnable(){
                     @Override
                     public void run() { processChildren(); }
                  }, resetDelay, TimeUnit.MILLISECONDS);
            }
         }
         
         private void initClusterInfoDefaultDirs() throws ClusterInfoException
         {
            //===========================================
            // Initialize the current structure
            // I don't want the manager to create the cluster dir since the creator
            // of the clusterdir is required to add the clusterinfo.
            if (session.exists(util.getClusterDir(), null))
            {
               session.mkdir(util.getNodesDir(), DirMode.PERSISTENT);
               session.mkdir(util.getShardsDir(), DirMode.PERSISTENT);
            }
            //===========================================
         }
         
         /**
          * <p>This method contains the micro-shard assignment logic. Currently there is a fixed
          * number of nodes required. Once elasticity is implemented then the node count will
          * be able to be dynamically increased. Until then the MicroShardClusterInformation
          * minNodeCount is the total node count.</p>
          * 
          * <p>This should only be called with the lock held.</p>
          * 
          * @param dirsToAdd is the new directories since we previously updated the state.
          * @param dirsToRemove is the missing directories since we previously updated the state.
          * @return true if successful.
          */
         private boolean rebalance(Set<String> dirsToAdd, Set<String> dirsToRemove) throws ClusterInfoException
         {
            DecentralizedRoutingStrategy.DefaultRouterClusterInfo ci = clusterInfo.get();
            long minNodeCount = ci == null ? -1 : ci.getMinNodeCount();
            if (minNodeCount <= 0)
               return false;
            
            long numShards = ci.getTotalShardCount();
            
            // This is what we do for elasticity. However, for now we will just make
            // the minNodeCount = the total node count.
            //long usingNodeCount = curNodeCount > minNodeCount ? curNodeCount : minNodeCount;
            
            // no matter what we need to remove the dirs that correspond to the dirsToRemove.
            Collection<String> remoteShards = session.getSubdirs(util.getShardsDir(), null);
            
            Map<String,String> shardToNodePath = new HashMap<String,String>();

            // save off the SlotInformation for later use.
            // TODO: (maybe) make sure the addition and deletion are done in conjunction which may prevent 
            // there temporarily being missing shard assignments.
            for(String shard : remoteShards)
            {
               MicroShardRoutingStrategy.MicroShardInfo slotInformation = (MicroShardRoutingStrategy.MicroShardInfo)session.getData(util.getShardsDir()+"/"+shard, null);
               if (slotInformation == null) // something weird happened ... let's fix it.
                  session.rmdir(util.getShardsDir() + "/" + shard);
               else
               {
                  // do we need to remove this slotInformation?
                  if (dirsToRemove.contains(slotInformation.getNodePath()))
                     session.rmdir(util.getShardsDir() + "/" + shard);
                  else
                     // if this is still a valid shard subdir then keep the slotinfo
                     shardToNodePath.put(shard, slotInformation.getNodePath());
               }
            }
            
            // now, how many shards are not assigned.
            Set<String> missingShards = new HashSet<String>();
            for (long i = 0; i < numShards; i++)
               missingShards.add("" + i);
            
            for (String assignedShard : shardToNodePath.keySet())
               missingShards.remove(assignedShard);
            
            if (missingShards.size() > 0)
            {

               // now figure out what to do with the rest of them.
               Map<String,NodeShardCount> nodePathToCount = new HashMap<String,NodeShardCount>();
               for (Map.Entry<String,String> entry : shardToNodePath.entrySet())
               {
                  String shard = entry.getKey();
                  String nodePath = entry.getValue();
                  NodeShardCount holder = nodePathToCount.get(nodePath);
                  if (holder == null)
                  {
                     holder = new NodeShardCount(nodePath);
                     nodePathToCount.put(nodePath, holder);
                  }
                  holder.shards.add(shard); // add an existing shard
               }

               // now lets add the dirsToAdd
               for (String nodePath : dirsToAdd)
               {
                  if (nodePathToCount.size() >= minNodeCount)
                     break; /// once we have too many the rest are just backups
                  nodePathToCount.put(nodePath, new NodeShardCount(nodePath));
               }

               List<NodeShardCount> holders = new ArrayList<NodeShardCount>(nodePathToCount.size());
               holders.addAll(nodePathToCount.values());

               // now add stub NodeShardCounts in order to get it to the minNodeCount
               // (which is currently simply the fixed node count).
               while (holders.size() < minNodeCount)
                  holders.add(new NodeShardCount()); // add a stub count for missing nodes.

               long countPerNode = numShards / minNodeCount; // this will round down.
               
               Set<String> stillMissingShards = new HashSet<String>();
               stillMissingShards.addAll(missingShards);
               for (String shard : missingShards)
               {
                  NodeShardCount toAddTo = findMin(holders);
                  if (toAddTo.size() <= countPerNode)
                  {
                     stillMissingShards.remove(shard);
                     toAddTo.newShards.add(shard);
                  }
                  else // the one with the lowest count has too many
                     break;
               }
               
               if (stillMissingShards.size() > 0 && logger.isDebugEnabled())
                  logger.debug("Assigned all shards for cluster " + SafeString.valueOf(clusterId) + " but have these leftover " + stillMissingShards);
               
               stillMissingShards.clear();

               // now actually do the assignment.
               for (NodeShardCount cur : holders)
               {
                  if (cur.newShards.size() > 0)
                  {
                     if (cur.nodePath == null) // then this is a stubbed out NodeShardCount
                        stillMissingShards.addAll(cur.newShards); // we are just keeping track for logging.
                     else // otherwise we can actually set the shard
                     {
                        // get the current slotInfo from the node directory
                        MicroShardRoutingStrategy.MicroShardInfo si = (MicroShardRoutingStrategy.MicroShardInfo)session.getData(util.getNodesDir() + "/" + cur.nodePath, null);
                        
                        // there is a potential race condition here. If si is null it may be that the 
                        // Node directory was added but the cluster information wasn't yet. In this case we fail and start over.
                        if (si == null)
                           return false;
                        
                        for (String newShard : cur.newShards)
                        {
                           // if this already exists, that would be odd, but ok
                           session.mkdir(util.getShardsDir() + "/" + newShard,DirMode.PERSISTENT);
                           session.setData(util.getShardsDir() + "/" + newShard, si);
                        }
                     }
                  }
               }
               
            }
            return true;
         }
      }
   }
   private static NodeShardCount findMin(Collection<NodeShardCount> col)
   {
      NodeShardCount ret = null;
      for (NodeShardCount cur : col)
      {
         if (ret == null || ret.shards.size() > cur.shards.size())
            ret = cur;
      }
      return ret;
   }
   
   private static class NodeShardCount
   {
      public String nodePath;
      public Set<String> shards = new HashSet<String>();
      public Set<String> newShards = new HashSet<String>();
      
      public NodeShardCount(String nodePath) { this.nodePath = nodePath; }
      public NodeShardCount() { this.nodePath = null; }
      public int size() { return shards.size() + newShards.size(); }
   }
   
   /**
    * This method will return the all of the entries from list1 that are 
    * "missing From" list2.
    */
   private static Set<String> missingFrom(Collection<String> list1, Collection<String> list2)
   {
      Set<String> ret = new HashSet<String>();
      for (String cur1 : list1)
         if (!list2.contains(cur1))
            ret.add(cur1);
      return ret;
   }

}
