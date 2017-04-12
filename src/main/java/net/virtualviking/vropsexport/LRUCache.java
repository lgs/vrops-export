package net.virtualviking.vropsexport;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private int cacheSize;

  public LRUCache(int cacheSize) {
    super(cacheSize, 0.75F, true);
    this.cacheSize = cacheSize;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
}