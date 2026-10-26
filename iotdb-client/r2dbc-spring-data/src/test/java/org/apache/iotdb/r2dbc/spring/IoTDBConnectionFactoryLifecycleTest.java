/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.r2dbc.spring;

import org.apache.iotdb.r2dbc.IoTDBR2dbcConnectionFactory;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Wrapped;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.r2dbc.ConnectionFactoryDecorator;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.scheduler.Scheduler;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Exercises real Boot factory creation without requiring a listening database server. */
public class IoTDBConnectionFactoryLifecycleTest {
  private static final String URL = "r2dbc:iotdb://127.0.0.1:1/lifecycle";

  @Test
  public void closesBootPoolBeforeItsDriverAndToleratesRepeatedContextClose() throws Exception {
    AtomicReference<Boolean> closedBeforePoolDisposal = new AtomicReference<>();
    ConfigurableApplicationContext context =
        application()
            .initializers(
                candidate ->
                    candidate
                        .getBeanFactory()
                        .addBeanPostProcessor(
                            new DestructionAwareBeanPostProcessor() {
                              @Override
                              public void postProcessBeforeDestruction(Object bean, String name) {
                                if (bean instanceof ConnectionPool) {
                                  closedBeforePoolDisposal.set(
                                      closed(driver((ConnectionFactory) bean)));
                                }
                              }

                              @Override
                              public boolean requiresDestruction(Object bean) {
                                return bean instanceof ConnectionPool;
                              }
                            }))
            .run();
    ConnectionPool pool = context.getBean(ConnectionPool.class);
    IoTDBR2dbcConnectionFactory driver = driver(pool);
    try {
      assertNotNull(context.getBean(R2dbcEntityTemplate.class));
      assertFalse(closed(driver));
      context.close();
      context.close();
      assertEquals(Boolean.FALSE, closedBeforePoolDisposal.get());
      assertTrue(pool.isDisposed());
      assertTrue(closed(driver));
      awaitRuntimeStopped(driver);
    } finally {
      context.close();
      driver.close();
    }
  }

  @Test
  public void closesBootOptionsWrapperWithoutPooling() throws Exception {
    ConfigurableApplicationContext context =
        application().properties("spring.r2dbc.pool.enabled=false").run();
    ConnectionFactory factory = context.getBean(ConnectionFactory.class);
    IoTDBR2dbcConnectionFactory driver = driver(factory);
    try {
      assertEquals(
          "org.springframework.boot.r2dbc.OptionsCapableConnectionFactory",
          factory.getClass().getName());
      context.close();
      context.close();
      assertTrue(closed(driver));
      awaitRuntimeStopped(driver);
    } finally {
      context.close();
      driver.close();
    }
  }

  @Test
  public void eachContextOwnsOnlyItsOwnBootFactory() throws Exception {
    ConfigurableApplicationContext first = application().run();
    ConfigurableApplicationContext second = application().run();
    IoTDBR2dbcConnectionFactory firstDriver = driver(first.getBean(ConnectionFactory.class));
    IoTDBR2dbcConnectionFactory secondDriver = driver(second.getBean(ConnectionFactory.class));
    try {
      first.close();
      assertTrue(closed(firstDriver));
      assertFalse(closed(secondDriver));
      second.close();
      assertTrue(closed(secondDriver));
      awaitRuntimeStopped(firstDriver);
      awaitRuntimeStopped(secondDriver);
    } finally {
      first.close();
      second.close();
      firstDriver.close();
      secondDriver.close();
    }
  }

  @Test
  public void leavesUserSuppliedPoolDelegateWithItsExternalOwner() throws Exception {
    IoTDBR2dbcConnectionFactory external =
        (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(URL);
    ConnectionPool pool =
        new ConnectionPool(ConnectionPoolConfiguration.builder(external).initialSize(0).build());
    try (ConfigurableApplicationContext context =
        application()
            .initializers(
                candidate -> {
                  RootBeanDefinition definition =
                      new RootBeanDefinition(ConnectionPool.class, () -> pool);
                  definition.setDestroyMethodName("dispose");
                  ((GenericApplicationContext) candidate)
                      .registerBeanDefinition("connectionFactory", definition);
                })
            .run()) {
      assertEquals(pool, context.getBean(ConnectionFactory.class));
      context.close();
      assertTrue(pool.isDisposed());
      assertFalse(closed(external));
    } finally {
      pool.dispose();
      external.close();
    }
  }

  @Test
  public void leavesUserSuppliedFactoryWithItsExternalOwner() throws Exception {
    IoTDBR2dbcConnectionFactory external =
        (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(URL);
    try (ConfigurableApplicationContext context =
        application()
            .initializers(
                candidate -> {
                  RootBeanDefinition definition =
                      new RootBeanDefinition(IoTDBR2dbcConnectionFactory.class, () -> external);
                  definition.setDestroyMethodName("");
                  ((GenericApplicationContext) candidate)
                      .registerBeanDefinition("connectionFactory", definition);
                })
            .run()) {
      context.close();
      assertFalse(closed(external));
    } finally {
      external.close();
    }
  }

  @Test
  public void honorsExplicitSpringOwnedDriverAndPoolDependency() throws Exception {
    IoTDBR2dbcConnectionFactory owned = (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(URL);
    ConnectionPool pool =
        new ConnectionPool(ConnectionPoolConfiguration.builder(owned).initialSize(0).build());
    try (ConfigurableApplicationContext context =
        application()
            .initializers(
                candidate -> {
                  GenericApplicationContext generic = (GenericApplicationContext) candidate;
                  RootBeanDefinition driver =
                      new RootBeanDefinition(IoTDBR2dbcConnectionFactory.class, () -> owned);
                  driver.setDestroyMethodName("close");
                  generic.registerBeanDefinition("iotdbDriver", driver);
                  RootBeanDefinition pooled =
                      new RootBeanDefinition(ConnectionPool.class, () -> pool);
                  pooled.setPrimary(true);
                  pooled.setDependsOn("iotdbDriver");
                  pooled.setDestroyMethodName("dispose");
                  generic.registerBeanDefinition("connectionFactory", pooled);
                })
            .run()) {
      context.close();
      assertTrue(pool.isDisposed());
      assertTrue(closed(owned));
    } finally {
      pool.dispose();
      owned.close();
    }
  }

  @Test
  public void releasesDriverWhenContextInitializationFailsAfterFactoryCreation() throws Exception {
    AtomicReference<IoTDBR2dbcConnectionFactory> captured = new AtomicReference<>();
    try {
      assertThrows(
          RuntimeException.class,
          () ->
              application()
                  .initializers(
                      candidate ->
                          ((GenericApplicationContext) candidate)
                              .registerBean(
                                  "failingBean",
                                  Object.class,
                                  () -> {
                                    captured.set(
                                        driver(candidate.getBean(ConnectionFactory.class)));
                                    throw new IllegalStateException(
                                        "expected initialization failure");
                                  }))
                  .run());
      assertNotNull(captured.get());
      assertTrue(closed(captured.get()));
      awaitRuntimeStopped(captured.get());
    } finally {
      if (captured.get() != null) {
        captured.get().close();
      }
    }
  }

  @Test
  public void releasesDriverWhenFactoryPostProcessingFails() throws Exception {
    AtomicReference<IoTDBR2dbcConnectionFactory> captured = new AtomicReference<>();
    try {
      assertThrows(
          RuntimeException.class,
          () ->
              application()
                  .initializers(
                      candidate ->
                          candidate
                              .getBeanFactory()
                              .addBeanPostProcessor(
                                  new BeanPostProcessor() {
                                    @Override
                                    public Object postProcessBeforeInitialization(
                                        Object bean, String name) {
                                      if (bean instanceof ConnectionFactory) {
                                        captured.set(driver((ConnectionFactory) bean));
                                        throw new IllegalStateException(
                                            "expected factory initialization failure");
                                      }
                                      return bean;
                                    }
                                  }))
                  .run());
      assertNotNull(captured.get());
      assertTrue(closed(captured.get()));
      awaitRuntimeStopped(captured.get());
    } finally {
      if (captured.get() != null) {
        captured.get().close();
      }
    }
  }

  @Test
  public void releasesDriverWhenBootPoolConstructionFailsBeforeBeanInitialization()
      throws Exception {
    AtomicReference<IoTDBR2dbcConnectionFactory> captured = new AtomicReference<>();
    try {
      assertThrows(
          RuntimeException.class,
          () ->
              application()
                  .properties("spring.r2dbc.pool.max-size=0")
                  .initializers(
                      candidate ->
                          ((GenericApplicationContext) candidate)
                              .registerBean(
                                  "captureFactory",
                                  ConnectionFactoryDecorator.class,
                                  () ->
                                      factory -> {
                                        captured.set(driver(factory));
                                        return factory;
                                      }))
                  .run());
      assertNotNull(captured.get());
      assertTrue(closed(captured.get()));
      awaitRuntimeStopped(captured.get());
    } finally {
      if (captured.get() != null) {
        captured.get().close();
      }
    }
  }

  private static SpringApplicationBuilder application() {
    return new SpringApplicationBuilder(Application.class)
        .web(WebApplicationType.NONE)
        .properties(
            "spring.r2dbc.url=" + URL,
            "spring.r2dbc.pool.initial-size=0",
            "spring.r2dbc.pool.max-size=1",
            "spring.main.banner-mode=off",
            "logging.level.root=OFF");
  }

  private static IoTDBR2dbcConnectionFactory driver(ConnectionFactory factory) {
    Object current = factory;
    while (current instanceof Wrapped<?>) {
      current = ((Wrapped<?>) current).unwrap();
    }
    return (IoTDBR2dbcConnectionFactory) current;
  }

  private static boolean closed(IoTDBR2dbcConnectionFactory driver) {
    try {
      Field field = IoTDBR2dbcConnectionFactory.class.getDeclaredField("closed");
      field.setAccessible(true);
      return ((AtomicBoolean) field.get(driver)).get();
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static void awaitRuntimeStopped(IoTDBR2dbcConnectionFactory driver) throws Exception {
    Field managerField = IoTDBR2dbcConnectionFactory.class.getDeclaredField("clientManager");
    managerField.setAccessible(true);
    Object manager = managerField.get(driver);
    Field selectorField = manager.getClass().getDeclaredField("selectThread");
    selectorField.setAccessible(true);
    Thread selector = (Thread) selectorField.get(manager);
    selector.join(5000);
    assertFalse(selector.isAlive());
    Field schedulerField = IoTDBR2dbcConnectionFactory.class.getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    assertTrue(((Scheduler) schedulerField.get(driver)).isDisposed());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @AutoConfigurationPackage(basePackages = "org.apache.iotdb.r2dbc.lifecycle.empty")
  static class Application {}
}
