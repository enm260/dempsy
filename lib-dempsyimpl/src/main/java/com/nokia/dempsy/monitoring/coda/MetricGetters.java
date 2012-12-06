package com.nokia.dempsy.monitoring.coda;

/**
 * This interface is to allow the getting of metrics within the tests.
 */
public interface MetricGetters
{
   long getProcessedMessageCount();
   long getDispatchedMessageCount();
   long getMessageFailedCount();
   long getDiscardedMessageCount();
   long getMessageCollisionCount();
   int getInFlightMessageCount();
   long getMessagesNotSentCount();
   long getMessagesSentCount();
   long getMessagesReceivedCount();
   double getPreInstantiationDuration();
   long getPreInstantiationCount();
   double getOutputInvokeDuration();
   double getEvictionDuration();
   long getMessageProcessorsCreated();
   
   long getMessageBytesSent();
   long getMessageBytesReceived();
   
   long getMessagesPending();
   long getMessagesOutPending();
}
