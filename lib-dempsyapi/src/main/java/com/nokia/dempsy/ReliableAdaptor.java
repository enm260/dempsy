package com.nokia.dempsy;

public interface ReliableAdaptor extends Adaptor
{
   public void ack(Object msgId);
   
   public void fail(Object msgId);
}
