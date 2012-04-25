package com.nokia.dempsy.guice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nokia.dempsy.Dempsy;
import com.nokia.dempsy.guice.modules.DempsyModule;

public class ApplicationDempsy
{
   private static Logger logger = LoggerFactory.getLogger(ApplicationDempsy.class);

   /**
    * @param args
    */
   public static void main(String[] args)
   {
      try
      {
         // find the application definition module
         String moduleClassName = args.length > 0 ? args[0] : null;
         if (moduleClassName == null)
            moduleClassName = System.getProperty("module");
         
         if (moduleClassName == null)
            usage("missing guice Module class");
         else
         {
            DempsyModule dempsyModule = new DempsyModule();

            Injector injector = Guice.createInjector(dempsyModule);
            Dempsy dempsy = injector.getInstance(Dempsy.class);
            dempsy.start();

            dempsy.waitToBeStopped();
         }
      }
      catch(Throwable e)
      {
         logger.error("Failed...", e);
      }
   }
   
   private static void usage(String errorMessage)
   {
      if (errorMessage != null)
      {
         logger.error(errorMessage);
         System.out.println(errorMessage);
      }
      
      String usageMessage = "java [params] -Dmodule=class.name.of.guice.Module " + ApplicationDempsy.class.getName();
      logger.error(usageMessage);
      System.out.println(usageMessage);
   }

}
