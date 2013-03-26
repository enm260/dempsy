package com.nokia.dempsy.reliable;

import com.nokia.dempsy.annotations.MessageKey;

public final class ReliabilityEvent
{
   private final Object msgId;
   private final Object dependentMsgId;
   private final EventCode code;
   
   public enum EventCode { Ack, Fail, New };
   
   public ReliabilityEvent(final Object msgId, final EventCode code)
   {
      this.msgId = msgId;
      this.code = code;
      this.dependentMsgId = null;
   }
   
   public ReliabilityEvent(final Object msgId, final Object dependentMsgId,
            final EventCode code)
   {
      this.msgId = msgId;
      this.code = code;
      this.dependentMsgId = dependentMsgId;
   }
   
   @MessageKey
   public final Object getMessageId() { return msgId; }
   
   public final Object getDependentMessageId() { return dependentMsgId; }
   
   public final EventCode getCode() { return code; }
}
