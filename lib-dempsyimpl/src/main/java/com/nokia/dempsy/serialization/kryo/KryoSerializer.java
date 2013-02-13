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

package com.nokia.dempsy.serialization.kryo;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy.ShardInfo;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

/**
 * This is the implementation of the Kryo based serialization for Dempsy.
 * It can be configured with registered classes using Spring by passing
 * a list of {@link Registration} instances to the constructor.
 */
public class KryoSerializer<T> extends Serializer<T> 
{
   private static Logger logger = LoggerFactory.getLogger(KryoSerializer.class);
   private static class KryoHolder
   {
      public Kryo kryo = new Kryo();
      public Output output = new Output(1024, 1024*1024*1024);
      public Input input = new Input();
   }
   // need an object pool of Kryo instances since Kryo is not thread safe
   private ConcurrentLinkedQueue<KryoHolder> kryopool = new ConcurrentLinkedQueue<KryoHolder>();
   private List<Registration> registrations = null;
   private ConcurrentLinkedQueue<KryoOptimizer> optimizer = new ConcurrentLinkedQueue<KryoOptimizer>();
   private boolean requireRegistration = false;
   
   /**
    * Create an unconfigured default {@link KryoSerializer} with no registered classes.
    */
   public KryoSerializer()
   {
      // This is an ugly hack ... need to find a better way to do it
      setKryoOptimizer(decentralizedRoutingStrategyShardInfoOptimizer);
   }
   
   /**
    * Create an {@link KryoSerializer} with the provided registrations. This can be
    * used from a Spring configuration.
    */
   public KryoSerializer(Registration... regs)
   {
      setKryoOptimizer(decentralizedRoutingStrategyShardInfoOptimizer);
      registrations = Arrays.asList(regs); 
   }
   
   /**
    * Create an {@link KryoSerializer} with the provided registrations and Application specific
    * Optimizer. This can be used from a Spring configuration.
    */
   public KryoSerializer(KryoOptimizer optimizer, Registration... regs)
   {
      setKryoOptimizer(decentralizedRoutingStrategyShardInfoOptimizer);
      registrations = Arrays.asList(regs);
      this.optimizer.add(optimizer);
   }
   
   /**
    * Set the optimizer. This is provided for a dependency injection framework to use. If it's called
    * @param optimizer
    */
   public synchronized void setKryoOptimizer(KryoOptimizer... optimizers)
   {
      for (KryoOptimizer cur : optimizers)
         this.optimizer.add(cur);
      kryopool.clear(); // need to create new Kryo's.
   }
   
   /**
    * You can require Kryo to serialize only registered classes by passing '<code>true</code>' to
    * setKryoRegistrationRequired. The default is '<code>false</code>'.
    */
   public synchronized void setKryoRegistrationRequired(boolean requireRegistration)
   {
      if (this.requireRegistration != requireRegistration)
      {
         this.requireRegistration = requireRegistration;
         kryopool.clear();
      }
   }

   @Override
   public byte[] serialize(T object) throws SerializationException 
   {
      return doserialize(object,null);
   }
   
   @Override
   public T deserialize(byte[] data) throws SerializationException 
   {
      return dodeserialize(data,null);
   }
   
   @Override
   public <E extends T> byte[] serialize(E object, Class<E> clazz) throws SerializationException
   {
      return doserialize(object,clazz); 
   }
   
   @SuppressWarnings("unchecked")
   public <E extends T> E deserialize(byte[] data, Class<E> clazz) throws SerializationException
   {
      return (E)dodeserialize(data,clazz); 
   }
   
   @SuppressWarnings("unchecked")
   private T dodeserialize(byte[] data, Class<?> clazz ) throws SerializationException
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         k.input.setBuffer(data);
         return (T)(clazz == null ? k.kryo.readClassAndObject(k.input) : k.kryo.readObject(k.input, clazz));  
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to deserialize.",ke);
      }
      catch (IllegalArgumentException e) // this happens when requiring registration but deserializing an unregistered class
      {
         throw new SerializationException("Failed to deserialize. Did you require registration and attempt to deserialize an unregistered class?", e);
      }
      finally
      {
         if (k != null)
            kryopool.offer(k);
      }
   }
   
   private byte[] doserialize(T object, Class<?> clazz) throws SerializationException
   {
      KryoHolder k = null;
      try
      {
         k = getKryoHolder();
         k.output.clear();
         if (clazz == null) k.kryo.writeClassAndObject(k.output, object);
         else k.kryo.writeObject(k.output,object);
         return k.output.toBytes();
      }
      catch (KryoException ke)
      {
         throw new SerializationException("Failed to serialize.",ke);
      }
      catch (IllegalArgumentException e) // this happens when requiring registration but serializing an unregistered class
      {
         throw new SerializationException("Failed to serialize " + SafeString.objectDescription(object) + 
               " (did you require registration and attempt to serialize an unregistered class?)", e);
      }
      finally
      {
         if (k != null)
            kryopool.offer(k);
      }
   }
   
   private KryoHolder getKryoHolder()
   {
      KryoHolder ret = kryopool.poll();
      if (ret == null)
      {
         ret = new KryoHolder();
         if (requireRegistration)
            ret.kryo.setRegistrationRequired(requireRegistration);
         
         if (optimizer != null)
         {
            for (KryoOptimizer cur : optimizer)
            {
               try { cur.preRegister(ret.kryo); }
               catch (Throwable th) { logger.error("Optimizer for KryoSerializer \"" + SafeString.valueOfClass(optimizer) + 
                     "\" threw and unepexcted exception.... continuing.",th); }
            }
         }

         if (registrations != null)
         {
            for (Registration reg : registrations)
            {
               try
               {
                  if (reg.id == -1)
                     ret.kryo.register(Class.forName(reg.classname));
                  else
                     ret.kryo.register(Class.forName(reg.classname), reg.id);
               }
               catch (ClassNotFoundException cnfe)
               {
                  logger.error("Cannot register the class " + SafeString.valueOf(reg.classname) + " with Kryo because the class couldn't be found.");
               }
            }
         }
         
         if (optimizer != null)
         {
            for (KryoOptimizer cur : optimizer)
            {
               try { cur.postRegister(ret.kryo); }
               catch (Throwable th) { logger.error("Optimizer for KryoSerializer \"" + SafeString.valueOfClass(optimizer) + 
                     "\" threw and unepexcted exception.... continuing.",th); }
            }
         }
      }
      return ret;
   }
   
   private static KryoOptimizer decentralizedRoutingStrategyShardInfoOptimizer = new KryoOptimizer()
   {
      @Override
      public void preRegister(Kryo kryo)
      {
         kryo.addDefaultSerializer(ShardInfo.class, new com.esotericsoftware.kryo.Serializer<ShardInfo>()
         {
            { this.setAcceptsNull(true); }
            
            @Override
            public void write(Kryo kryo, Output output, ShardInfo object) { output.writeInt(object.getShard()); }

            @Override
            public ShardInfo read(Kryo kryo, Input input, Class<ShardInfo> type) { return new ShardInfo(input.readInt()); }
         });
      }
      
      @Override
      public void postRegister(Kryo kryo) { }
   };
   


}
