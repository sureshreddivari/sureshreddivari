package com.mdsol.ctms.keystore;


import static com.mdsol.ctms.keystore.KeyGenConstants.REDIS_KEYGEN_LOCK_WAIT_TIME_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.mdsol.ctms.dao.KeyGenDAO;

import uhuru.matrix.ConfigException;
import uhuru.matrix.Matrix;
import uhuru.matrix.cache.redis.RedisCacheClient;
import uhuru.matrix.config.Component;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyGenCacheHelper.class, Matrix.class, KeyGenDAO.class})
public class KeyGenCacheManagerTest {

  private static final String CURRENT_ID = "currentId";
  private static final String RESET_ON_ID = "resetOn";
  private static final long INCREMENTED_ID = 1061001;
  private static final long TEST_RESET_ON_ID = 1061100;
  private static final int poolSize = 100;
  private static final String ENV_CURRENT_ID= "ctms-currentId";
  private static final String ENV_RESET_ON_ID= "ctms-resetOn";

  private KeyGenCacheManager keyGenCacheManager;
  @Mock
  private Component component;
  @Mock
  private RedissonClient redissonClient;
  @Mock
  private RedisCacheClient redisCacheClient;
  @Mock
  private RBucket<Object> currentIdBucket;
  @Mock
  private RBucket<Object> resetBucket;
  @Mock
  private KeyGenDAO keyGenDAO;
  @Mock
  private RLock rLock;

  @Before
  public void setUp() throws IOException, ConfigException {
    mockStatic(KeyGenCacheHelper.class);
    when(KeyGenCacheHelper.getRedisCacheClient()).thenReturn(redisCacheClient);
    doReturn(redissonClient).when(redisCacheClient).getRedissonClient();
    mockStatic(Matrix.class);
    when(Matrix.getSpringBean(KeyGenDAO.class)).thenReturn(keyGenDAO);
    when(redisCacheClient.createKey(eq(CURRENT_ID))).thenReturn(ENV_CURRENT_ID);
    when(redisCacheClient.createKey(eq(RESET_ON_ID))).thenReturn(ENV_RESET_ON_ID);
    doReturn(currentIdBucket).when(redissonClient).getBucket(ENV_CURRENT_ID);
    doReturn(resetBucket).when(redissonClient).getBucket(ENV_RESET_ON_ID);
    when(Matrix.getProperty(anyString(), anyString())).thenReturn(REDIS_KEYGEN_LOCK_WAIT_TIME_VALUE);

    keyGenCacheManager = spy(new KeyGenCacheManager());
  }

  @Test
  public void test_get_instance() throws Exception {
    KeyGenCacheManager instance = KeyGenCacheManager.getInstance();
    assertNotNull(instance);
    assertEquals(instance, KeyGenCacheManager.getInstance());
  }

  @Test
  public void test_configure() throws IOException, ConfigException {
    doReturn(INCREMENTED_ID).when(currentIdBucket).get();
    doReturn(TEST_RESET_ON_ID).when(resetBucket).get();
    mockStatic(KeyGenCacheHelper.class);
    when(KeyGenCacheHelper.isRedisKeyGenEnabled()).thenReturn(true);
    doReturn(true).when(redisCacheClient).isConnected();
    doReturn(INCREMENTED_ID).when(keyGenDAO).getCurrentId();
    when(KeyGenCacheHelper.getKeyGenPoolSize()).thenReturn(poolSize);

    keyGenCacheManager.configure(component);
    verify(redisCacheClient, times(1)).getRedissonClient();
    verify(redisCacheClient, times(1)).isConnected();
    assertEquals(INCREMENTED_ID, Long.parseLong(String.valueOf(currentIdBucket.get())));
    assertEquals(TEST_RESET_ON_ID, Long.parseLong(String.valueOf(resetBucket.get())));
  }

  @Test
  public void test_configure_reset() throws IOException, ConfigException {
    doReturn(true).when(currentIdBucket).trySet(anyLong());
    doReturn(true).when(resetBucket).trySet(anyLong());
    mockStatic(KeyGenCacheHelper.class);
    when(KeyGenCacheHelper.isRedisKeyGenEnabled()).thenReturn(true);
    doReturn(true).when(redisCacheClient).isConnected();
    keyGenCacheManager.configure(component);
    verify(redisCacheClient, times(1)).getRedissonClient();
    verify(redisCacheClient, times(1)).isConnected();
    verify(keyGenDAO, times(1)).getAndIncrementCurrentId(anyInt());
  }

  @Test
  public void test_configure_else_case() throws IOException, ConfigException {
    mockStatic(KeyGenCacheHelper.class);
    when(KeyGenCacheHelper.isRedisKeyGenEnabled()).thenReturn(false);
    doReturn(true).when(redisCacheClient).isConnected();
    keyGenCacheManager.configure(component);
    verify(currentIdBucket, times(1)).delete();
    verify(resetBucket, times(1)).delete();
  }

  @Test(expected = FailToAcquireDistributedLockException.class)
  public void tes_lock_exception_next() throws Exception {
    doReturn(rLock).when(redissonClient).getLock(anyString());
    keyGenCacheManager.nextId();
  }

  @Test
  public void test_next() throws Exception {
    doReturn(rLock).when(redissonClient).getLock(anyString());
    doReturn(INCREMENTED_ID).when(currentIdBucket).get();
    doReturn(TEST_RESET_ON_ID).when(resetBucket).get();
    doReturn(true).when(rLock).tryLock(anyLong(), anyLong(), anyObject());
    keyGenCacheManager.nextId();
    verify(currentIdBucket, times(1)).set(anyInt());
  }

  @Test
  public void test_next_reset() throws Exception {
    doReturn(rLock).when(redissonClient).getLock(anyString());
    doReturn(INCREMENTED_ID).when(currentIdBucket).get();
    doReturn(INCREMENTED_ID).when(resetBucket).get();
    doReturn(true).when(rLock).tryLock(anyLong(), anyLong(), anyObject());

    keyGenCacheManager.nextId();
    verify(keyGenDAO, times(1)).getAndIncrementCurrentId(anyInt());
  }
}
