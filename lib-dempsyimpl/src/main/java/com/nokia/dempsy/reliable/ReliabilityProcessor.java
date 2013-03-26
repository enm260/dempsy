package com.nokia.dempsy.reliable;

import com.nokia.dempsy.ReliableAdaptor;
import com.nokia.dempsy.TestWordCount.WordCounter;
import com.nokia.dempsy.annotations.Activation;
import com.nokia.dempsy.annotations.MessageHandler;
import com.nokia.dempsy.annotations.MessageProcessor;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.messagetransport.MessageTransportException;
import com.nokia.dempsy.messagetransport.Sender;
import com.nokia.dempsy.messagetransport.SenderFactory;

@MessageProcessor(pooled=true)
public final class ReliabilityProcessor implements Cloneable
{
   private final ReliableAdaptor adaptor;
   private Object msgId;
   private long state;
   
   public ReliabilityProcessor(ReliableAdaptor adaptor)
   {
      this.adaptor = adaptor;
   }

   @MessageHandler
   public void handleAck(ReliabilityEvent ack) throws MessageTransportException
   {
      switch (ack.getCode())
      {
         case Ack:
            break;
         case Fail:
            break;
         case New:
            
            break;
         default:
            break;
         
      }
   }
   
   @Activation
   public void init(Object msgId) { this.msgId = msgId; }
   
   @Override
   public ReliabilityProcessor clone() throws CloneNotSupportedException 
   { return (ReliabilityProcessor) super.clone(); }
   
}
