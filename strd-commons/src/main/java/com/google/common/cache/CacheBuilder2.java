package com.google.common.cache;

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 17/04/14
 * Time: 15:17
 */
public class CacheBuilder2<K,V> {

    public final CacheBuilder builder = CacheBuilder.newBuilder();

    public LoadingCache<K, V> build(
          CacheLoader<K, V> loader) {
        return builder.build(loader);
      }
}
