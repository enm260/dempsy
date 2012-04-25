package com.nokia.dempsy.guice.testapp;

import org.junit.Ignore;

import com.google.inject.AbstractModule;
import com.nokia.dempsy.config.ApplicationDefinition;

@Ignore
public class Module extends AbstractModule
{

   @Override
   protected void configure()
   {
      /*ApplicationDefinition ret =*/ new ApplicationDefinition("test");
   }

}
