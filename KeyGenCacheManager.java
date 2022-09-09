package com.mdsol.ctms.keystore;

import static com.mdsol.ctms.keystore.KeyGenConstants.CURRENT_ID;
import static com.mdsol.ctms.keystore.KeyGenConstants.DELIMITER_HYPHEN;
import static com.mdsol.ctms.keystore.KeyGenConstants.KEYGEN_PREFIX;
import static com.mdsol.ctms.keystore.KeyGenConstants.REDIS_KEYGEN_LOCK_LEASE_TIME;
import static com.mdsol.ctms.keystore.KeyGenConstants.REDIS_KEYGEN_LOCK_LEASE_TIME_VALUE;
import static com.mdsol.ctms.keystore.KeyGenConstants.REDIS_KEYGEN_LOCK_WAIT_TIME;
import static com.mdsol.ctms.keystore.KeyGenConstants.REDIS_KEYGEN_LOCK_WAIT_TIME_VALUE;
import static com.mdsol.ctms.keystore.KeyGenConstants.RESET_ON_ID;
import static java.lang.String.format;

import com.mdsol.ctms.dao.KeyGenDAO;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import uhuru.matrix.ConfigException;
import uhuru.matrix.IConfiglet;
import uhuru.matrix.LoggingObject;
import uhuru.matrix.Matrix;
import uhuru.matrix.cache.redis.RedisCacheClient;
import uhuru.matrix.config.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KeyGenCacheManager extends LoggingObject implements IConfiglet {

  private static KeyGenCacheManager instance;
  private RedisCacheClient redisCacheClient;
  private RedissonClient redissonClient;
  private KeyGenDAO keyGenDAO;

  final String currentIdKey;
  final String resetOnIdKey;
  final String redissonLockId;
  public KeyGenCacheManager() {
    redisCacheClient = KeyGenCacheHelper.getRedisCacheClient();
    redissonClient = redisCacheClient.getRedissonClient();
    keyGenDAO = Matrix.getSpringBean(KeyGenDAO.class);
    currentIdKey = redisCacheClient.createKey(CURRENT_ID);
    resetOnIdKey = redisCacheClient.createKey(RESET_ON_ID);
    redissonLockId = String.join(DELIMITER_HYPHEN, Matrix.getAppName(), "KEY-GEN-LOCK");
    setLogName(IConfiglet.LOG_NAME);
  }

  public static KeyGenCacheManager getInstance() {
    if (instance == null) {
      instance = new KeyGenCacheManager();
    }
    return instance;
  }

  @Override
  public void configure(Component component) throws ConfigException, IOException {

    RBucket<Object> currentIdBucket;
    RBucket<Object> resetBucket;
    instance = this;

    if (redisCacheClient != null && redisCacheClient.isConnected()) {

      currentIdBucket = redissonClient.getBucket(currentIdKey);
      resetBucket = redissonClient.getBucket(resetOnIdKey);

      if (KeyGenCacheHelper.isRedisKeyGenEnabled()) {
        info("Redis keygen configuration started");

        // for multi-node repeated add just fails & avoid key_gen update
        KeyStore keyStore = new KeyStore().getInstance();
        long currentId = keyStore.getCurrentId();
        long lastId = keyStore.getLastId();

        boolean isCurrentIdAdded = currentIdBucket.trySet(currentId);
        boolean isLastIdAdded = resetBucket.trySet(lastId);

        if (isCurrentIdAdded && isLastIdAdded) {
          updateKeyGenTableEntry();
        }

        info(format("[%s] - ADD - Redis KeyGen : isCurrentIdAdded - %s, isResetOnIdAdded - %s", KEYGEN_PREFIX, isCurrentIdAdded, isLastIdAdded));
        info(format("[%s] - ADD - Redis KeyGen : currentId - %s, resetOnId - %s", KEYGEN_PREFIX, currentIdBucket.get(), resetBucket.get()));
        info(format("[%s] - ADD - CTMS : currentId - %s", KEYGEN_PREFIX, keyGenDAO.getCurrentId()));
        info(format("Redis keygen lock name : %s", redissonLockId));
      } else {
        info("Redis server is up, but disabling keygen configuration");
        info(format("[%s] - DELETE - Redis KeyGen : currentId - %s, resetOnId - %s", KEYGEN_PREFIX, currentIdBucket.get(), resetBucket.get()));

        currentIdBucket.delete();
        resetBucket.delete();
      }
    } else {
      error(format("[%s] - Failed to configure %s component - Redis KegGen is disabled", KEYGEN_PREFIX, component.getName()));
    }
  }

  private void updateKeyGenTableEntry() {
    keyGenDAO.getAndIncrementCurrentId(KeyGenCacheHelper.getKeyGenPoolSize());
  }

  public long nextId() throws InterruptedException, FailToAcquireDistributedLockException {
    int lockWaitTime = Integer.parseInt(Matrix.getProperty(REDIS_KEYGEN_LOCK_WAIT_TIME, REDIS_KEYGEN_LOCK_WAIT_TIME_VALUE));
    int lockLeaseTime = Integer.parseInt(Matrix.getProperty(REDIS_KEYGEN_LOCK_LEASE_TIME, REDIS_KEYGEN_LOCK_LEASE_TIME_VALUE));
    long cacheKeyId = 0L;
    if (isDebugOn()) {
      debug(format("redis.keygen.lock.waitTime : %d, redis.keygen.lock.leaseTime : %d", lockWaitTime, lockLeaseTime));
    }
    RLock lock = redissonClient.getLock(redissonLockId);
    //Wait for $lockWaitTim seconds and automatically unlock it after $lockLeaseTime seconds
    if (!lock.tryLock(lockWaitTime, lockLeaseTime, TimeUnit.SECONDS)) {
      throw new FailToAcquireDistributedLockException("Failed to acquire redis distributed lock");
    }

    try {
      RBucket<Object> currentIdBucket = redissonClient.getBucket(currentIdKey);
      RBucket<Object> resetBucket = redissonClient.getBucket(resetOnIdKey);

      cacheKeyId = Long.parseLong(String.valueOf(currentIdBucket.get()));
      long cacheResetOnId = Long.parseLong(String.valueOf(resetBucket.get()));

      if (cacheKeyId == cacheResetOnId) {
        if (isDebugOn()) {
          debug(format("[%s] - [%s] - RESET - Redis KeyGen : Performing reload on - %s", Thread.currentThread().getName(), KEYGEN_PREFIX, cacheKeyId));
        }
        resetKeyGenCache();
        cacheKeyId = Long.parseLong(String.valueOf(currentIdBucket.get()));
      }

      // update cache to incremented id
      currentIdBucket.set(cacheKeyId + 1);
      if (isDebugOn()) {
        debug(format("[%s] - [%s] - NEXT - Redis KeyGen : nextId - %s", Thread.currentThread().getName(), KEYGEN_PREFIX, currentIdBucket.get()));
      }
    } catch (Exception exception) {
      error("Failed to get ID from redis keygen: ", exception);
    } finally {
      lock.unlock();
    }
    return cacheKeyId;
  }

  private void resetKeyGenCache() {
    KeyStore keyStore = new KeyStore().getInstance();
    long currentId = keyStore.getCurrentId();
    long lastId = keyStore.getLastId();

    RBucket<Object> currentIdBucket = redissonClient.getBucket(currentIdKey);
    RBucket<Object> resetBucket = redissonClient.getBucket(resetOnIdKey);

    currentIdBucket.set(currentId);
    resetBucket.set(lastId);

    updateKeyGenTableEntry();

    if (isDebugOn()) {
      debug(format("[%s] - [%s] - RESET - Redis KeyGen : currentId - %s, resetOnId - %s", Thread.currentThread().getName(), KEYGEN_PREFIX, currentIdBucket.get(), resetBucket.get()));
    }
  }

  private class KeyStore {
    private long currentId;
    private long lastId;

    public long getCurrentId() {
      return currentId;
    }

    public long getLastId() {
      return lastId;
    }

    public KeyStore getInstance() {
      currentId = keyGenDAO.getCurrentId();
      int poolSize = KeyGenCacheHelper.getKeyGenPoolSize();
      lastId = currentId + poolSize;
      return this;
    }
  }

  public long getCurrentIdFromCache() {
    return Long.parseLong(String.valueOf(redissonClient.getBucket(currentIdKey).get())) - 1;
  }

}
