package com.nokia.dempsy;

public interface ReliableDispatcher extends Dispatcher
{
   public void dispatch(Object message, Object messageId);
}
