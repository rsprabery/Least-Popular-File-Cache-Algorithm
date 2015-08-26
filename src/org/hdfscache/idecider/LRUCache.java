package org.hdfscache.idecider;

import java.util.TreeMap;

/**
 * Created by read on 8/26/15.
 */
public class LRUCache implements Cache {

    private final TreeMap<Long, Inode> lruHash = new TreeMap<Long, Inode>();
    private int CACHE_SIZE = 500;

    @Override
    public void read(Inode file) {
        synchronized (lruHash) {
            synchronized (file) {
                // Increment the access count
                file.incrementAndSetAccesscount();

                // set the current access time
                file.setAccesstime(System.currentTimeMillis());

                // Check if file is already cached or not. If yes then hit else miss
                if (file.isCached()) {
                    // Hit
                } else { //miss
                    if (cacheable(file)) {
                        addToCache(file);
                    }
                }
            }
        }
    }


    /*
    This check is the definition of an LRU cache, but isn't really needed. The fact this is getting called means that
    it has been accessed more recently than the smallest value in the tree hash.
     */
    public boolean cacheable(Inode file) {
        // if the file's access time is is sooner than the lowest entry, then replace the lowest entry.
        if (lruHash.size() < CACHE_SIZE) {
            return true;
        } else {
            return file.getAccesstime() > lruHash.firstKey();
        }
    }

    /*
    Here, if the cache is full, we remove the first (lowest) entry and mark the Inode as not cached.

    We then add the inode "file" passed in to the cache and mark it as cached.
     */
    public void addToCache(Inode file) {
        if (!(lruHash.size() < CACHE_SIZE)) {
            // if it's full, remove the least recently used file and then insert our new one.
            lruHash.firstEntry().getValue().setCached(false);
            lruHash.remove(lruHash.firstKey());
        }
        lruHash.put(file.getAccesstime(), file);
        file.setCached(true);
    }

}
