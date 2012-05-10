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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

/**
 * This Routing Strategy uses the {@link MpCluster} to negotiate with other instances in the 
 * cluster.
 */
public class DefaultRoutingStrategy implements RoutingStrategy
{
   private static Logger logger = LoggerFactory.getLogger(DefaultRoutingStrategy.class);
   
   private int defaultTotalSlots;
   private int defaultNumNodes;
   private Set<Class<?>> routerMap = new HashSet<Class<?>>();
   
   public DefaultRoutingStrategy(int defaultTotalSlots, int defaultNumNodes)
   {
      this.defaultTotalSlots = defaultTotalSlots;
      this.defaultNumNodes = defaultNumNodes;
   }
   
   private class Outbound implements RoutingStrategy.Outbound, MpClusterWatcher<ClusterInformation, SlotInformation>
   {
      private ConcurrentHashMap<Integer, Destination> destinations = new ConcurrentHashMap<Integer, Destination>();
      private int totalAddressCounts = -1;
      private RoutingStrategy.Outbound.Coordinator coordinator;
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      private ClusterId clusterId;
      private transient boolean isSetup = false;
      
      private Outbound(RoutingStrategy.Outbound.Coordinator coordinator,
            MpCluster<ClusterInformation, SlotInformation> cluster)
      {
         this.coordinator = coordinator;
         this.cluster = cluster;
         this.clusterId = cluster.getClusterId();
         cluster.addWatcher(this);
         // force a call to process to update
         setup()
         process(cluster);
      }

      @Override
      public Destination selectDestinationForMessage(Object messageKey, Object message) throws DempsyException
      {
         if (totalAddressCounts < 0)
            throw new DempsyException("It appears the Outbound strategy for the message key " + 
                  SafeString.objectDescription(messageKey) + 
                  " is being used prior to initialization.");
         int calculatedModValue = Math.abs(messageKey.hashCode()%totalAddressCounts);
         return destinations.get(calculatedModValue);
      }
      
      @Override
      public synchronized void process(MpCluster<ClusterInformation, SlotInformation> clusterHandle)
      {
         if (logger.isTraceEnabled())
            logger.trace("Resetting Outbound Strategy for cluster " + clusterId);
         
         destinations.clear();
         Map<Integer, DefaultRouterSlotInfo> slotInfoMap = new HashMap<Integer, DefaultRouterSlotInfo>();
         int newtotalAddressCounts;
         try
         {
            newtotalAddressCounts = fillMapFromActiveSlots(slotInfoMap,clusterHandle);
            if (newtotalAddressCounts == 0)
               logger.error("The cluster " + clusterHandle.getClusterId() + 
                     " seems to have invalid slot information. Someone has set the total number of slots to zero.");
            totalAddressCounts = newtotalAddressCounts > 0 ? newtotalAddressCounts : totalAddressCounts;
         }
         catch(MpClusterException e)
         {
            logger.error("Major problem with the Router. Can't respond to changes in the cluster information:", e);
         }
      }

      private void setup(MpCluster<ClusterInformation, SlotInformation> clusterHandle)
      {
         if (!isSetup || force)
         {
            synchronized(this)
            {
               if (!isSetup || force) // double checked locking?
               {
                  isSetup = false;

                  Collection<MpClusterSlot<SlotInformation>> nodes = clusterHandle.getActiveSlots();
                  if(nodes != null)
                  {
                     Set<Class<?>> msgClass = null;
                     for(MpClusterSlot<SlotInformation> node: nodes)
                     {
                        SlotInformation slotInfo = node.getSlotInformation();
                        if(slotInfo != null)
                        {
                           msgClass = slotInfo.getMessageClasses();
                           if (msgClass != null)
                              messageClasses.addAll(msgClass);
                        }
                     }
                     
                     // now we may have new messageClasses so we need to register with the Router
                     for (Class<?> clazz : messageClasses)
                     {
                        Set<ClusterRouter> cur = Collections.newSetFromMap(new ConcurrentHashMap<ClusterRouter, Boolean>()); // potential
                        Set<ClusterRouter> tmp = routerMap.putIfAbsent(clazz, cur);
                        if (tmp != null)
                           cur = tmp;
                        cur.add(this);
                     }

                  }
               }
            }
         }
      }

   } // end Outbound class definition
   
   private class Inbound implements RoutingStrategy.Inbound, MpClusterWatcher<ClusterInformation,SlotInformation>
   {
      private List<Integer> destinationsAcquired = new ArrayList<Integer>();
      private Destination destination;
      private List<Class<?>> messageTypes;
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      private ClusterId clusterId;
      
      private Inbound(MpCluster<ClusterInformation, SlotInformation> cluster,
            List<Class<?>> messageTypes, Destination thisDestination)
      {
         this.cluster = cluster;
         this.clusterId = cluster.getClusterId();
         this.destination = thisDestination;
         this.messageTypes = messageTypes;
         cluster.addWatcher(this);
      }

      @Override
      public synchronized void process(MpCluster<ClusterInformation, SlotInformation> clusterHandle)
      {
         if (logger.isTraceEnabled())
            logger.trace("Resetting Inbound Strategy for cluster " + clusterId);

         int minNodeCount = defaultNumNodes;
         int totalAddressNeeded = defaultTotalSlots;
         Random random = new Random();
         
         try
         {
            //==============================================================================
            // need to verify that the existing slots in destinationsAcquired are still ours
            Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
            fillMapFromActiveSlots(slotNumbersToSlots,clusterHandle);
            Collection<Integer> slotsToReaquire = new ArrayList<Integer>();
            for (Integer destinationSlot : destinationsAcquired)
            {
               // select the coresponding slot information
               DefaultRouterSlotInfo slotInfo = slotNumbersToSlots.get(destinationSlot);
               if (slotInfo == null || !destination.equals(slotInfo.getDestination()))
                  slotsToReaquire.add(destinationSlot);
            }
            //==============================================================================

            //==============================================================================
            // Now reaquire the potentially lost slots
            for (Integer slotToReaquire : slotsToReaquire)
            {
               if (!acquireSlot(slotToReaquire, totalAddressNeeded, clusterHandle, messageTypes, destination))
               {
                  // in this case, see if I already own it...
                  logger.error("Cannot reaquire the slot " + slotToReaquire + " for the cluster " + clusterHandle.getClusterId());
               }
            }
            //==============================================================================

            while(needToGrabMoreSlots(clusterHandle,minNodeCount,totalAddressNeeded))
            {
               int randomValue = random.nextInt(totalAddressNeeded);
               if(destinationsAcquired.contains(randomValue))
                  continue;
               if (acquireSlot(randomValue, totalAddressNeeded,
                     clusterHandle, messageTypes, destination))
                  destinationsAcquired.add(randomValue);                  
            }
         }
         catch (MpClusterException e)
         {
            
         }
      }
      
      private boolean needToGrabMoreSlots(MpCluster<ClusterInformation, SlotInformation> clusterHandle,
            int minNodeCount, int totalAddressNeeded) throws MpClusterException
      {
         int addressInUse = clusterHandle.getActiveSlots().size();
         int maxSlotsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)minNodeCount);
         return addressInUse < totalAddressNeeded && destinationsAcquired.size() < maxSlotsForOneNode;
      }
      
      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return destinationsAcquired.contains(messageKey.hashCode()%defaultTotalSlots);
      }
      
   } // end Inbound class definition
   
   @Override
   public RoutingStrategy.Inbound createInbound(MpCluster<ClusterInformation, SlotInformation> cluster,
         List<Class<?>> messageTypes, Destination thisDestination) 
   {
         return new Inbound(cluster, messageTypes, thisDestination);
   }
   
   @Override
   public RoutingStrategy.Outbound createOutbound(RoutingStrategy.Outbound.Coordinator coordinator,
            MpCluster<ClusterInformation, SlotInformation> cluster)
   {
      return new Outbound(coordinator,cluster); 
   }
   
   static class DefaultRouterSlotInfo extends SlotInformation
   {
      private static final long serialVersionUID = 1L;

      private int totalAddress = -1;
      private int slotIndex = -1;

      public int getSlotIndex() { return slotIndex; }
      public void setSlotIndex(int modValue) { this.slotIndex = modValue; }

      public int getTotalAddress() { return totalAddress; }
      public void setTotalAddress(int totalAddress) { this.totalAddress = totalAddress; }

      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = super.hashCode();
         result = prime * result + (int)(slotIndex ^ (slotIndex >>> 32));
         result = prime * result + (int)(totalAddress ^ (totalAddress >>> 32));
         return result;
      }

      @Override
      public boolean equals(Object obj)
      {
         if (!super.equals(obj))
            return false;
         DefaultRouterSlotInfo other = (DefaultRouterSlotInfo)obj;
         if(slotIndex != other.slotIndex)
            return false;
         if(totalAddress != other.totalAddress)
            return false;
         return true;
      }
   }
   
   static class DefaultRouterClusterInfo extends ClusterInformation
   {
      private static final long serialVersionUID = 1L;

      private AtomicInteger minNodeCount = new AtomicInteger(5);
      private AtomicInteger totalSlotCount = new AtomicInteger(300);
      
      public DefaultRouterClusterInfo(int totalSlotCount, int nodeCount)
      {
         this.totalSlotCount.set(totalSlotCount);
         this.minNodeCount.set(nodeCount);
      }

      public int getMinNodeCount() { return minNodeCount.get(); }
      public void setMinNodeCount(int nodeCount) { this.minNodeCount.set(nodeCount); }

      public int getTotalSlotCount(){ return totalSlotCount.get();  }
      public void setTotalSlotCount(int addressMultiplier) { this.totalSlotCount.set(addressMultiplier); }
   }
   
   /**
    * Fill the map of slots to slotinfos for internal use. 
    * @return the totalAddressCount from each slot. These are supposed to be repeated.
    */
   private static int fillMapFromActiveSlots(Map<Integer,DefaultRouterSlotInfo> mapToFill, 
         MpCluster<ClusterInformation, SlotInformation> clusterHandle)
   throws MpClusterException
   {
      int totalAddressCounts = -1;
      Collection<MpClusterSlot<SlotInformation>> slotsFromClusterManager = clusterHandle.getActiveSlots();

      if(slotsFromClusterManager != null)
      {
         for(MpClusterSlot<SlotInformation> node: slotsFromClusterManager)
         {
            DefaultRouterSlotInfo slotInfo = (DefaultRouterSlotInfo)node.getSlotInformation();
            if(slotInfo != null)
            {
               mapToFill.put(slotInfo.getSlotIndex(), slotInfo);
               if (totalAddressCounts == -1)
                  totalAddressCounts = slotInfo.getTotalAddress();
               else if (totalAddressCounts != slotInfo.getTotalAddress())
                  logger.error("There is a problem with the slots taken by the cluster manager for the cluster " + 
                        clusterHandle.getClusterId() + ". Slot " + slotInfo.getSlotIndex() +
                        " from " + SafeString.objectDescription(slotInfo.getDestination()) + 
                        " thinks the total number of slots for this cluster it " + slotInfo.getTotalAddress() +
                        " but a former slot said the total was " + totalAddressCounts);
            }
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireSlot(int slotNum, int totalAddressNeeded,
         MpCluster<ClusterInformation, SlotInformation> clusterHandle,
         List<Class<?>> messagesTypes, Destination destination) throws MpClusterException
   {
      MpClusterSlot<SlotInformation> slot = clusterHandle.join(String.valueOf(slotNum));
      if(slot == null)
         return false;
      DefaultRouterSlotInfo dest = (DefaultRouterSlotInfo)slot.getSlotInformation();
      if(dest == null)
      {
         dest = new DefaultRouterSlotInfo();
         dest.setDestination(destination);
         dest.setSlotIndex(slotNum);
         dest.setTotalAddress(totalAddressNeeded);
         dest.setMessageClasses(messagesTypes);
         slot.setSlotInformation(dest);
      }
      return true;
   }

}
