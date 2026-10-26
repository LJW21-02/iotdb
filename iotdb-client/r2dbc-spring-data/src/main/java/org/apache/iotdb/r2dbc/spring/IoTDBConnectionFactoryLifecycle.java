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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Wrapped;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.r2dbc.ConnectionFactoryDecorator;
import org.springframework.core.PriorityOrdered;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Context-local ownership, acquired only through Boot's factory creation extension point. */
final class IoTDBConnectionFactoryLifecycle
    implements ConnectionFactoryDecorator,
        BeanPostProcessor,
        BeanNameAware,
        DisposableBean,
        PriorityOrdered {
  private final ConfigurableListableBeanFactory beanFactory;
  private final Set<IoTDBR2dbcConnectionFactory> factories =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private String beanName;
  private boolean destroyed;

  IoTDBConnectionFactoryLifecycle(ConfigurableListableBeanFactory beanFactory) {
    this.beanFactory = beanFactory;
  }

  @Override
  public void setBeanName(String beanName) {
    this.beanName = beanName;
  }

  @Override
  public int getOrder() {
    // Observe the original Boot wrapper before application decorators add other wrappers.
    return HIGHEST_PRECEDENCE;
  }

  @Override
  public synchronized ConnectionFactory decorate(ConnectionFactory factory) {
    Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Object current = factory;
    while (seen.add(current)) {
      if (current instanceof IoTDBR2dbcConnectionFactory) {
        IoTDBR2dbcConnectionFactory driver = (IoTDBR2dbcConnectionFactory) current;
        if (destroyed) {
          driver.close();
        } else {
          factories.add(driver);
        }
        break;
      }
      if (!(current instanceof Wrapped<?>)) {
        break;
      }
      current = ((Wrapped<?>) current).unwrap();
    }
    return factory;
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String name) {
    if (bean instanceof ConnectionFactory) {
      // Spring must dispose pools and their connections before stopping the driver's runtime.
      // Merely observing a user factory here never transfers ownership of its delegate.
      beanFactory.registerDependentBean(beanName, name);
    }
    return bean;
  }

  @Override
  public synchronized void destroy() {
    destroyed = true;
    factories.forEach(IoTDBR2dbcConnectionFactory::close);
    factories.clear();
  }
}
