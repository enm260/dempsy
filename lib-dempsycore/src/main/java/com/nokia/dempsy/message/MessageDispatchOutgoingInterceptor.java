package com.nokia.dempsy.message;

/**
 * This is an interface that represents an internal message dispatcher.
 * Implementations are interceptors that are part of a stack that handle
 * messages leaving a node, and coming into a node.  
 */
public interface MessageDispatchOutgoingInterceptor
{
   public void setNextInterceptor(MessageDispatchOutgoingInterceptor next);
   
   public void dispatch(Object message, Object key, MessageBufferOutput output);
}
