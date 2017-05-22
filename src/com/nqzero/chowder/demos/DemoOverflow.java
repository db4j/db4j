// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.chowder.demos;

import kilim.Pausable;
import org.db4j.Command;
import org.db4j.Db4j;
import org.db4j.HunkArray;
import static org.db4j.perf.DemoHunker.resolve;
import org.srlutils.Simple;
import org.srlutils.Util;

/*
 * at the time of writing this class
 * Hunker keeps track of the generation in an int, and increments it for every transaction
 *   and again for every write
 *   querunner.genc
 * it overflows (31 bits)
 * need to fix the overflow - should wrap the genc
 *   need to update outstanding txns and the cache
 * this class is left here as a non-trivial example of the overflow (takes several hours ???)
 *   should also probably write a trivial one that overflows quicker
 *
 * run 'DemoEntropy init' first to build up tables
 */
public class DemoOverflow {
    String filename = resolve("./db_files/b6.mmap");
    Db4j db4j;
    HunkArray.I map;
    void load() {
        db4j = Db4j.load(filename);
        map = (HunkArray.I) db4j.lookup(0);
    }
    public void init() {
        db4j = new Db4j().init( filename, null );
        map = db4j.register(new HunkArray.I(),"Player Overflow");
        db4j.create();
        db4j.fence(null,100);
        db4j.forceCommit(100);
    }
    org.srlutils.rand.Source r1 = new org.srlutils.rand.Source();
    { 
        Long seed = null;
        r1.setSeed(seed,true);
    }
    long abs(int [] vals) {
        long abs = 0;
        for (int ii=1; ii < nv; ii++) abs += Math.abs(vals[ii]);
        return abs;
    }
    long sum(int [] vals) {
        long sum = Util.Ranged.sum(1,vals.length,vals);
        return sum;
    }
    /** number of players           */ int np = 1<<22; // also, range of quantities
    /** number of values per player */ int nv = 1<< 4;
    public void doinit() {
        init();
        for (int ii = 0; ii < np; ii++) {
            final int kplayer = ii;
            final int [] vals = r1.rand(new int[nv],1-np,np);
            long sum = sum(vals), delta = sum/(nv-1);
            for (int jj=2; jj < vals.length; jj++, sum -= delta)
                vals[jj] -= delta;
            vals[2] -= sum;
            Simple.softAssert(sum(vals)==0);
            new Db4j.Query() {
                public void task() throws Pausable {
                    map.setdata(tid,kplayer*nv,vals,new Command.RwInts().init(true),nv);
                }
            }.offer(db4j);
        }
        db4j.fence(null,10);
        System.out.println("insert complete");
    }
    long running;
    int count, limit = 1<<14;
    public void rotate(int nn) {
        for (int ii = 0; ii < nn; ii++) {
            dorotate();
            System.out.format( "rotate completed %5d\n", ii );
        }
    }
    public void dorotate() {
        for (int ii = 0; ii < np; ii++)
            rot0(ii);
        db4j.fence(null,10);
    }
    void swap1(int [] d1,int [] d2,int k1,int k2) {
        int delta = (d1[k1] - d2[k2])/2;
        d1[k1] -= delta;
        d2[k2] += delta;
    }
    void check(int [] data) {
        long sum = sum(data);
        if (sum != 0)
            Simple.softAssert(sum==0);
        long abs = abs(data);
        running += abs;
        if (++count==limit) {
            System.out.format( "running: %7d\n", running );
            running = count = 0;
        }
    }
    public void rot0(int ii) {
        final int kplayer = ii;
        final int [] vals = r1.rand(new int[2],1,nv);
        if (vals[0]==vals[1]) return;
        new Db4j.Query() {
            public void task() throws Pausable {
                int [] data = new int[nv];
                map.setdata(tid,kplayer*nv,data,new Command.RwInts(),nv);
                yield();
                check(data);
                swap1(data,data,vals[0],vals[1]);
                map.setdata(tid,kplayer*nv,data,new Command.RwInts().init(true),nv);
            }
        }.offer(db4j);
    }
    public void close() {
        db4j.shutdown();
        db4j.close();
    }
    public static class Demo {
        public static void main(String [] args) {
            Simple.Scripts.cpufreqStash( 2300000 );
            DemoOverflow test = new DemoOverflow();
            if (false)
                test.doinit();
            else {
                test.load();
                test.rotate(1000);
            }
            test.close();
            
        }
    }
    
}
