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

package com.nokia.dempsy.messagetransport.tcp;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import com.nokia.dempsy.messagetransport.MessageTransportException;

public class TcpServer
{
   private static Logger logger = LoggerFactory.getLogger(TcpServer.class);
   protected TcpDestination destination;
   
   private ServerSocket serverSocket;
   private Thread serverThread;
   private AtomicBoolean stopMe = new AtomicBoolean(false);
   
   private Set<ClientThread> clientThreads = new HashSet<ClientThread>();
   
   private Object eventLock = new Object();
   private volatile boolean eventSignaled = false;
   
   protected boolean useLocalhost = false;
   protected int port = -1;
   
   public static final int maxReceivers = 256;
   
   protected TcpReceiver[] receivers = new TcpReceiver[maxReceivers];
   protected int numReceivers = 0;
   
   protected synchronized void start() throws MessageTransportException
   {
      if (isStarted())
         return;
      
      // this sets the destination instance
      getDestination();
      
      // we need to call bind here in case the getDestination didn't do it
      //  (which it wont if the port is not ephemeral)
      bind();
      
      // in case this is a restart, we want to reset the stopMe value.
      stopMe.set(false);
      
      serverThread = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            while (!stopMe.get())
            {
               try
               {
                  // Wait for an event one of the registered channels
                  Socket clientSocket = serverSocket.accept();

                  // at the point we're committed to adding a new ClientThread to the set.
                  //  So we need to lock it.
                  synchronized(clientThreads)
                  {
                     // unless we're done.
                     if (!stopMe.get())
                     {
                        // This should come from a thread pool
                        ClientThread clientThread = makeNewClientThread(clientSocket);
                        Thread thread = new Thread(clientThread, "Client Handler for " + getClientDescription(clientSocket));
                        thread.setDaemon(true);
                        thread.start();

                        clientThreads.add(clientThread);
                     }
                  }

               }
               // This can happen if I rip the socket out from underneath the accept call. 
               // Because accept doesn't exit with a Thread.interrupt call so closing the server
               // socket from another thread is the only way to make this happen.
               catch (SocketException se)
               {
                  // however, if we didn't explicitly stop the server, then there's another problem
                  if (!stopMe.get()) 
                     logger.error("Socket error on the server managing " + destination, se);
               }
               catch (Throwable th)
               {
                  logger.error("Major error on the server managing " + destination, th);
               }
            }

            // we're leaving so signal
            synchronized(eventLock)
            {
               eventSignaled = true;
               eventLock.notifyAll();
            }
         }
      }, "Server for " + destination);
      
      serverThread.start();
    }
   
   protected synchronized void stop()
   {
      stopMe.set(true);
      
      if (serverThread != null)
         serverThread.interrupt();
      serverThread = null;
      
      // this is a hack because the thread interrupt doesn't wake 
      // up an accept call
      closeQuietly(serverSocket);
      serverSocket = null;
      
      synchronized(clientThreads)
      {
         for (ClientThread ct : clientThreads)
            ct.stop();
         
         clientThreads.clear();
      }
      
      // now wait until the event is signaled
      synchronized(eventLock)
      {
         if (!eventSignaled)
         {
            try { eventLock.wait(500); } catch(InterruptedException e) { }// wait for 1/2 second
         }
         
         if (!eventSignaled)
            logger.warn("Couldn't release the socket accept for " + destination);
      }
      
   }
   
   protected synchronized TcpDestination register(TcpReceiver receiver) throws MessageTransportException
   {
      TcpDestination nextDestination = makeNextDestination();
      this.receivers[nextDestination.sequence] = receiver;
      start();
      numReceivers++;
      return nextDestination;
   }
   
   protected synchronized void unregister(TcpDestination destination)
   {
      numReceivers--;
      if (numReceivers == 0)
         stop();

      this.receivers[destination.sequence] = null;
   }
   
   protected synchronized boolean isStarted()
   {
      return serverThread != null;
   }
   
   protected synchronized TcpDestination getDestination() throws MessageTransportException
   {
      if (destination == null)
         destination = doGetDestination();

      if (destination.isEphemeral())
         bind();
      
      return destination;
   }
      
   private TcpDestination doGetDestination() throws MessageTransportException
   {
      try
      {
         InetAddress inetAddress = useLocalhost ? InetAddress.getLocalHost() : TcpTransport.getFirstNonLocalhostInetAddress();
         if (inetAddress == null)
            throw new MessageTransportException("Failed to set the Inet Address for this host. Is there a valid network interface?");
         
         return new TcpDestination(inetAddress, port);
      }
      catch(UnknownHostException e)
      {
         throw new MessageTransportException("Failed to identify the current hostname", e);
      }
      catch(SocketException e)
      {
         throw new MessageTransportException("Failed to identify the current hostname", e);
      }
   }

   /**
    * Directs the factory to create TcpDestinations using "localhost." By default
    * the factory will not do this since the use of this InetAddress from another
    * machine will not result in a connection back to the machine the TcpDestination
    * was created from.
    */
   public void setUseLocalhost(boolean useLocalhost) { this.useLocalhost = useLocalhost; }
   
   /**
    * <p>Directs the factory to create TcpDestinations indicating that the port number is dynamic.
    * It does this by setting the port to -1. This is actually the default if the port number
    * isn't set using {@code setPort()}.</p>
    * 
    * <p>This is meant to be used from a DI container and set as a property. The value passed
    * doesn't actually do anything. To use a fixed port call setPort which will undo this setting.</p>
    */
   public void setUseEphemeralPort(boolean useEphemeralPort) { this.port = -1; }
   
   /**
    * Directs the factory to create TcpDestinations providing the given port number.
    */
   public void setPort(int port) { this.port = port; }
   
   protected void bind() throws MessageTransportException
   {
      if (serverSocket == null || !serverSocket.isBound())
      {
         try
         {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(destination.inetAddress, destination.port < 0 ? 0 : destination.port);
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true); // this allows the server port to be bound to even if it's in TIME_WAIT
            serverSocket.bind(inetSocketAddress);
            destination.port = serverSocket.getLocalPort();
         }
         catch(IOException ioe) 
         { 
            throw new MessageTransportException("Cannot bind to port " + 
                  (destination.isEphemeral() ? "(ephemeral port)" : destination.port),ioe);
         }
      }
   }
   
   private synchronized TcpDestination makeNextDestination() throws MessageTransportException
   {
      // find the next open place for a receiver.
      int receiverIndex = -1;
      for (receiverIndex = 0; receiverIndex < maxReceivers; receiverIndex++)
      {
         if (receivers[receiverIndex] == null)
            break;
      }
      
      if (receiverIndex >= maxReceivers)
         throw new MessageTransportException("Maximum number of receiver reached.");
      
      return new TcpDestination(getDestination(),receiverIndex);
   }
   
   // protected to be overridden in tests
   protected ClientThread makeNewClientThread(Socket clientSocket) throws IOException
   {
      return new ClientThread(clientSocket);
   }
   
   protected class ClientThread implements Runnable
   {
      private Socket clientSocket;
      private DataInputStream dataInputStream;
      private Thread thisThread;
      protected AtomicBoolean stopClient = new AtomicBoolean(false);
      
      protected ClientThread(Socket clientSocket) throws IOException
      { 
          this.clientSocket = clientSocket;
          this.dataInputStream = new DataInputStream( new BufferedInputStream( clientSocket.getInputStream() ) );
      }
      
      @Override
      public void run()
      {
         thisThread = Thread.currentThread();
         InputStream inputStream = null;
         Exception clientIsApparentlyGone = null;
         
         try
         {
            while (!stopMe.get() && !stopClient.get())
            {
               try
               {
                  int receiverToCall = -1;
                  byte[] messageBytes = null;
                  try
                  {
                     receiverToCall = dataInputStream.read(); // read a single unsigned byte
                     int size = dataInputStream.readShort();
                     if (size == -1)
                        size = dataInputStream.readInt();
                     if (size < 0)
                        // assume we have a corrupt channel
                        throw new IOException("Read negative message size. Assuming a corrupt channel.");
                        
                     messageBytes = new byte[ size ];
                     dataInputStream.readFully( messageBytes );
                  }
                  // either a problem with the socket OR a thread interruption (InterruptedIOException)
                  catch (EOFException eof)
                  {
                      clientIsApparentlyGone = eof;
                      messageBytes = null; // no message if exception
                  }
                  catch (IOException ioe)
                  {
                      clientIsApparentlyGone = ioe;
                      messageBytes = null; // no message if exception
                  }

                  if (messageBytes != null)
                  {
                     TcpReceiver receiver = receivers[receiverToCall];
                     if (receiver == null)  // it's possible we're shutting down
                     {
                        logger.error("Message received for a mising receiver. Unless we're shutting down, this shouldn't happen.");
                        stopClient.set(true);
                     }
                     else
                        receiver.handleMessage(messageBytes);
                  }
                  else if (clientIsApparentlyGone == null)
                  {
                     if (logger.isDebugEnabled())
                        logger.debug("Received a null message on destination " + destination);
                     
                     // if we read no bytes we should just consider ourselves lucky that we
                     // escaped a blocking read.
                     stopClient.set(true); // leave the loop.
                  }

                  if (clientIsApparentlyGone != null)
                  {
                     String logmessage = "Client " + getClientDescription(clientSocket) + 
                     " has apparently disconnected from sending messages to " + 
                     destination ;
                     if (logger.isDebugEnabled())
                        logger.debug(logmessage, clientIsApparentlyGone);
                     // assume the client socket is dead.

                     stopClient.set(true); // leave the loop.
                     clientIsApparentlyGone = null;
                  }
               }
               catch (Throwable th)
               {
                  logger.error(MarkerFactory.getMarker("FATAL"), "Completely unexpected error. This problem should be addressed because we should never make it here in the code.", th);
                  stopClient.set(true); // leave the loop, close up.
               }
            } // end while loop
         }
         catch (Throwable ue)
         {
            logger.error("Completely unexpected error", ue);
         }
         finally
         {
            if (inputStream != null) IOUtils.closeQuietly(inputStream);
            closeQuietly(clientSocket);

            // remove me from the client list.
            synchronized(clientThreads)
            {
               clientThreads.remove(this);
            }
         }
      }
      
      public void stop()
      {
         stopClient.set(true);
         if (thisThread != null)
            thisThread.interrupt();
         
         // close this SOB
         closeQuietly(clientSocket);
      }
   }
   
   private static String getClientDescription(Socket socket)
   {
      try
      {
         return "(" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + ")";
      }
      catch (Throwable th)
      {
         return "(Unknown Client)";
      }
   }
   
   private void closeQuietly(Socket socket) 
   {
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close socket failed for " + destination, ioe); 
         }
      }
   }

   private void closeQuietly(ServerSocket socket) 
   {
      if (socket != null)
      {
         try { socket.close(); } 
         catch (IOException ioe)
         {
            if (logger.isDebugEnabled())
               logger.debug("close socket failed for " + destination, ioe); 
         }
      }
   }

}
