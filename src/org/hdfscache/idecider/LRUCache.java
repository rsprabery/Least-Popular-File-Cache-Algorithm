package org.hdfscache.idecider;

import java.util.Comparator;
import java.util.NavigableSet;

import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

/**
 * Created by read on 8/26/15.
 */
public class LRUCache implements Cache {

    // key is the access time, value is an Inode or list of Inodes at that acess time
    private final TreeMultimap<Long, Inode> lruHash = TreeMultimap.create(new LongComparator(), new InodeComparator());
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
            return file.getAccesstime() > lruHash.keySet().first();
        }
    }

    /*
    Here, if the cache is full, we remove the first (lowest) entry and mark the Inode as not cached.

    We then add the inode "file" passed in to the cache and mark it as cached.
     */
    public void addToCache(Inode file) {

        if (!(lruHash.size() < CACHE_SIZE)) {
            // if it's full, remove the least recently used file and then insert our new one.
            // if there are multiple elements with the same access time, we'll grab the one
            // with the lowest id.
            Inode oldEntry = lruHash.get(lruHash.keySet().first()).first();
            synchronized (oldEntry) {
                oldEntry.setCached(false);
                lruHash.remove(lruHash.keySet().first(), oldEntry);
            }
        }

        lruHash.put(file.getAccesstime(), file);
        file.setCached(true);
    }

}

class InodeComparator implements Comparator<Inode> {
    @Override
    public int compare(Inode o1, Inode o2) {
        return (int)(o1.getInodeId() - o2.getInodeId());
    }
}

class LongComparator implements Comparator<Long> {
    @Override
    public int compare(Long o1, Long o2) {
        return o1.intValue() - o2.intValue();
    }
}
