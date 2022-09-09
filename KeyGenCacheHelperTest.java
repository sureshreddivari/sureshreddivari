package com.mdsol.ctms.keystore;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uhuru.matrix.Matrix;
import uhuru.matrix.cache.redis.RedisCacheClient;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Matrix.class, RedisCacheClient.class, KeyGenCacheHelper.class})
public class KeyGenCacheHelperTest {

  @Mock
  private RedisCacheClient redisCacheClient;

  @Mock
  private KeyGenCacheHelper keyGenCacheHelper;

  @Before
  public void setUp() {
    keyGenCacheHelper = new KeyGenCacheHelper();
    PowerMockito.mockStatic(Matrix.class);
    PowerMockito.mockStatic(RedisCacheClient.class);
    when(Matrix.getProperty(anyString(), anyString())).thenReturn("11111");
    when(RedisCacheClient.getInstance(anyInt(), anyInt(), anyInt())).thenReturn(redisCacheClient);
  }

  @Test
  public void getRedisCacheClient() {
    redisCacheClient = keyGenCacheHelper.getRedisCacheClient();
    verify(redisCacheClient, times(3)).getInstance(anyInt(), anyInt(), anyInt());
    assertEquals(redisCacheClient, RedisCacheClient.getInstance(anyInt(), anyInt(), anyInt()));
  }

  @Test
  public void isRedisConnected() {
    when(redisCacheClient.isConnected()).thenReturn(true);
    Boolean value = keyGenCacheHelper.isRedisConnected();
    verify(redisCacheClient, times(1)).isConnected();
    assertTrue(value);
  }

  @Test
  public void getKeyGenPoolSize() {
    assertEquals(11111, keyGenCacheHelper.getKeyGenPoolSize());
  }

  @Test
  public void isRedisKeyGenEnabled() {
    assertFalse(keyGenCacheHelper.isRedisKeyGenEnabled());
  }
}
