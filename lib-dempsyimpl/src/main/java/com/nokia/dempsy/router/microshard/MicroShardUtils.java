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

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;

public class MicroShardUtils
{
   private static String appRootDir = "/applications";
   private static String managerDir = "/managers";
   
   private String appDir;
   private String clusterDir;
   private String shardsDir;
   private String transitionRequestDir;
   private String transitionOfferDir;
   
   public MicroShardUtils(ClusterId clusterId)
   {
      this.appDir = getAppRootDir() + "/" + clusterId.getApplicationName();
      this.clusterDir = getAppDir() + "/" + clusterId.getMpClusterName();
      this.shardsDir = getClusterDir()+"/shards";
      this.transitionRequestDir = getClusterDir() + "/transitionRequest";
      this.transitionOfferDir = getClusterDir() + "/transitionOffer";
   }

   /**
    * The Application Root directory (appDir) is the parent directory for
    * all application directories.
    */
   public String getAppRootDir() { return appRootDir; }

   /**
    * The Application directory (appDir) is the root for the Microsharding
    * assignments for all of the clusters in the Dempsy Application for the 
    * {@link ClusterId} given in the constructor.
    */
   public String getAppDir() { return appDir; }

   /**
    * The ClusterDirectory is a subdirectory of the Application directory. It's named
    * by the clusterName from the given {@link ClusterId} and it contains an instance
    * of a {@link MicroShardClusterInformation}. It also serves as the root directory
    * for the cluster's nodes and shards subdirectories.
    */
   public String getClusterDir() { return clusterDir; }

   /**
    * A subdirectory of the ClusterDir, the nodesDir contains an ephemeral and sequential 
    * entry (subdirectory) per currently running node. Each of these subdirectories contains
    * an instance of a {@link DefaultShardInfo} which the manager will use to copy into 
    * the appropriate shardsDir subdirectory in order to accomplish an assignment.
    */
   
   public String getTransistionOfferDir() { return transitionOfferDir; }

   public String getTransistionRequestDir() { return transitionRequestDir; }

   public String getShardsDir() { return shardsDir; }

   public String getManagerDir() { return managerDir; }
   
   public String getRootDir() { return "/"; }
   
   public String mkAppRootDir(ClusterInfoSession session) throws ClusterInfoException
   {
      return session.mkdir(getAppRootDir(), DirMode.PERSISTENT);
   }
   
   public final String mkAppDir(ClusterInfoSession session) throws ClusterInfoException
   {
      mkAppRootDir(session);
      return session.mkdir(getAppDir(), DirMode.PERSISTENT);
   }
   
   public final String mkClusterDir(ClusterInfoSession session, Object obj) throws ClusterInfoException
   {
      mkAppDir(session);
      String ret = session.mkdir(getClusterDir(), DirMode.PERSISTENT);
      if (obj != null)
         session.setData(getClusterDir(), obj);
      return ret;
   }
   
   public final void mkAllPersistentAppDirs(ClusterInfoSession session,Object obj) throws ClusterInfoException
   {
      mkClusterDir(session,obj);
      session.mkdir(getShardsDir(), DirMode.PERSISTENT);
      session.mkdir(getTransistionRequestDir(), DirMode.PERSISTENT);
      session.mkdir(getTransistionOfferDir(), DirMode.PERSISTENT);
   }
   
   public final void mkManagerDir(ClusterInfoSession session) throws ClusterInfoException
   {
      session.mkdir(managerDir, DirMode.PERSISTENT);
   }
   
   /**
    * This uses the ClusterInfoSession to indicate to other members of the cluster that the caller
    * is requesting a shard since there is an imbalance.
    */
   public final String requestAShard(ClusterInfoSession session) throws ClusterInfoException
   {
      return session.mkdir(getTransistionRequestDir() + "/request_", DirMode.EPHEMERAL_SEQUENTIAL);
   }
   
   /**
    * Given that a request was made using makeRequest, the string returned can be used to 
    * see if any offers have been provided.
    */
   public final String findFirstOfferGivenRequest(ClusterInfoSession session, String requestDirectory, 
         ClusterInfoWatcher transitionOfferWatcher) throws ClusterInfoException
   {
      // get all of the outstanding offers
      Collection<String> offers = session.getSubdirs(getTransistionOfferDir(), transitionOfferWatcher);

      String ret = null;
      for (String cur : offers)
      {
         if (cur.startsWith(requestDirectory + "_"))
         {
            ret = cur;
            break;
         }
      }
      return ret;
   }
   
   public final String makeOffer(ClusterInfoSession session, String requestDirectory)
   {
      
   }
   
   public final int extractShardFromOfferDirectory(String madeOfferDir, String requestDir)
   {
      
   }
}
