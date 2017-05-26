// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import org.srlutils.DynArray;
import org.srlutils.Rand;
import org.srlutils.Simple;
import org.srlutils.TaskTimer;
import org.srlutils.data.TreeDisk;




/*
    a speed test of the old treedisk-based cache for db4j
    that cache is no longer used, so the implementation has been moved here
    the test is left here as an example of testing a cache
    the xorshift has the nice property that values don't repeat
    which allows testing for false positives
*/
public class CacheSpeed {
    
    public static class Tester extends TaskTimer.Runner<Integer> {
        /** leading source */ public Rand.XorShift x1 = new Rand.XorShift();
        /** lagging source */ public Rand.XorShift x2 = new Rand.XorShift();
        /** null    source */ public Rand.XorShift x3 = new Rand.XorShift();
        public int k3;
        public int cacheSize, cbits;
        public int nn = 400000, nc = 70;
        public int seed;
        public boolean ok;
        /** a short name describing the test        */  public String name = "cache";
        CacheTree ct;

        /** return a blurb describing the test */
        public String info() { return String.format( "%2s.%02d.%02d", name, cbits, nc ); }
        { stageNames = "run".split( " " ); }

        public void initMap() {
            ct = new CacheTree();
        }
        public void init() {
            seed = Rand.irand();
            x1.seed( seed );
            x2.seed( seed );
            x3.seed( seed );
            initMap();
            for (int ii = 0; ii < cacheSize; ii++) {
                CachedBlock cb = cb(x1.next());
                ct.put(cb);
            }
            ok = true;
            k3 = 0;
        }
        
        long bb = 4, bm = (1<<bb)-1;
        public CachedBlock cb(long val) {
            int gen = (int) (val & bm);
            long kb = val >> bb;
            CachedBlock cb = new CachedBlock();
            cb.setKey(kb,gen);
            return cb;
        }

        // x1 and x2 give the same series of unique values ... x1 leads by cacheSize
        // check that the leading value isn't already present
        // remove the lagging value
        // add the leading value
        public void run(int stage) throws Exception {
            CachedBlock k1, k2, clead, clag;
            for (int ii = 0; ii < nn; ii++) {
                for (int jj = 0; jj < nc; jj++) {
                    long val = x3.next();
                    CachedBlock cb = cb(val);
                    cb = ct.get(cb);
                    // what's in the cache is [ii,ii+cacheSize)
                    boolean should = k3>=ii && k3 < ii+cacheSize, did = cb != null;
                    if (should != did)
                        ok = false;
                    if (++k3 == nn+cacheSize) { x3.seed(seed); k3 = 0; }
                }
                long lagging = x2.next();
                long leading = x1.next();
                k1 = cb(leading);
                k2 = cb(lagging);
                clead = ct.get(k1);
                clag = ct.remove( k2 );
                if (clag==null || clag.compareTo(k2) != 0 || clead != null) ok = false;
                ct.put(k1);
            }
        }

        public boolean finish() throws Exception {
            initMap();
            return ok;
        }

        /** set the number of bits of cache and number of elements to test */
        public Tester setup(int $cbits,int $nn, int $nc) {
            cbits = $cbits;
            cacheSize = 1<<cbits;
            nn = $nn;
            nc = $nc;
            super.setup( stageNames.length, info() );
            return this;
        }
    }
    public static void main(String [] args) throws Exception {
        Simple.Scripts.cpufreq( "userspace", 3000000 );
        // all randomness should cascade from source, so setting seed to non-null should be deterministic
        Long seed = null;
        org.srlutils.Rand.source.setSeed( seed, true );
        int n2 = 1 << 20;

        n2 = 400000;

        TaskTimer tt = new TaskTimer().config(1).init( 3, 2, true, true );
        tt.width = 5;
        tt.dec = 3;

        int nc = 70;
        DynArray.Objects<Tester> da = new DynArray.Objects().init( Tester.class );
        for (int ii = 13, jj = 0; ii < 14; ii++, jj++) {
            da.add( new Tester().setup( ii, n2, nc ) );
        }

        tt.autoTimer( da.trim() );
        Simple.Scripts.cpufreq( "ondemand", 0 );
    }

    
    /**
     *  a tree storing cached blocks
     * 
     *  Note: this tree was once used by Db4j as the cache, but has long since been unused
     */
    public static class CacheTree extends TreeDisk<CachedBlock,Void> {
        /** returns 1 if v1 > v2 */
        public int compare(CachedBlock v1, CachedBlock v2,Void cc) { return v1.compareTo( v2 ); }
    }


    /** a stripped down version of Db4j.CachedBlock to support this speed test */
    public static class CachedBlock implements Comparable<CachedBlock> {
        long kblock;
        /** the generation of the latest write or zero for pure reads */
        long gen = 0;

        /** the the key */
        public CachedBlock setKey(long $kblock,long $gen)
            { kblock = $kblock; gen = $gen; return this; }
        /** sort by kblock, then gen */
        public int compareTo(CachedBlock o) {
            if (true)
                return Long.signum( kblock - o.kblock );
            return (kblock == o.kblock)
                    ? Long.signum( gen-o.gen )
                    : Long   .signum( kblock - o.kblock );
        }
    }
    
}



// at 3Ghz, 10 seconds for 400000 put/del, 70 reads per put
//        cache.13.70  | 9.179
// 2016:  cache.13.70  | 4.872 (CacheTree no longer used by Db4j)
//        cache.13.70  | 4.873 (after cleanup)

