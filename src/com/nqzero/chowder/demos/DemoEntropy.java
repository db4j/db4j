// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.chowder.demos;

import kilim.Pausable;
import org.db4j.Command;
import org.db4j.Db4j;
import org.db4j.HunkArray;
import org.db4j.HunkCount;
import static org.db4j.perf.DemoHunker.resolve;
import org.srlutils.DynArray;
import org.srlutils.Shuffler;
import org.srlutils.Simple;
import org.srlutils.Util;

public class DemoEntropy {
    String filename = resolve("./db_files/b6.mmap");
    Db4j db4j;
    Db4j.Connection conn;
    HunkArray.I map;
    HunkCount buildSeed;
    static final String PATH_MAP = "///db4j/demoentropy/map";
    static final String PATH_COUNT = "///db4j/demoentropy/count";

    void load() {
        db4j = Db4j.load(filename);
        conn = db4j.connect();
        db4j.submitCall(txn -> {
            map = txn.lookup(PATH_MAP);
            buildSeed = txn.lookup(PATH_COUNT);
        }).awaitb();
    }
    public void init() {
        db4j = new Db4j(filename, null); // 1L << 32 );
        conn = db4j.connect();
        db4j.submitCall(txn -> {
            map = txn.create(new HunkArray.I(), PATH_MAP);
            buildSeed = txn.create(new HunkCount(), PATH_COUNT);
        }).awaitb();
        Db4j.zygote(db4j).forceCommit(100);
    }
    org.srlutils.rand.Source r1 = new org.srlutils.rand.Source(), r2 = new org.srlutils.rand.Source();
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
    /** rotations per pass          */ int nr = 1<<16;
    /** number of players           */ int np = 1<<25; // also, range of quantities
    /** number of values per player */ int nv = 1<< 4;
    /** range of init values        */ int rmax = 1<<20;
    public void doinit() {
        init();
        final int bs = (int) r1.nextLong();
        build(bs);
        conn.submitCall(txn -> { buildSeed.set(txn,bs); });
        for (int ii = 0; ii < np; ii++) {
            final int kplayer = ii;
            final int [] vals = r1.rand(new int[nv],1-rmax,rmax);
            vals[0] = order[ii];
            long sum = sum(vals), delta = sum/(nv-1);
            for (int jj=2; jj < vals.length; jj++, sum -= delta)
                vals[jj] -= delta;
            vals[2] -= sum;
            Simple.softAssert(sum(vals)==0);
            new Db4j.Query() {
                public void task() throws Pausable {
                    map.setdata(txn,kplayer*nv,vals,new Command.RwInts().init(true),nv);
                }
            }.offer(conn);
        }
        conn.awaitb();
        System.out.println("insert complete");
        build();
    }
    int [] order;
    DynArray.ints robins;
    public void build(long seed) {
        order = new int[np];
        robins = new DynArray.ints();
        org.srlutils.rand.Source source = new org.srlutils.rand.Source();
        source.setSeed(seed);
        int kplayer = 0, avail = np;
        int stash = 0;
        for (; avail > 0; kplayer++) {
            while (order[kplayer] != 0)
                kplayer++;
            robins.add(kplayer);
            int num = source.rand(4,20);
            if (num > avail) num = avail;
            avail -= num;
            if (avail < 4) { num += avail; avail = 0; }
            int k1=kplayer, k2=0;
            for (int ii=0; ii < num-1; ii++, k1=k2) {
                order[k1] = -1;
                do { k2 = source.rand(kplayer+1,np); } while(order[k2] != 0);
                order[k1] = k2;
            }
            order[k1] = kplayer;
            if (kplayer==0)
                order[stash=k1] = -1;
        }
        order[stash] = 0;
        boolean check = false;
        if (check) {
            int [] test = new int[np];
            for (int ii=0; ii < np; ii++) test[order[ii]]++;
            System.out.format("done %5d %5d\n",Util.min(test),Util.max(test));
        }
    }
    void validateSums() {
        for (int ii = 0; ii < robins.size; ii++) {
            int kplayer = robins.get(ii);
            long sum = sums[kplayer];
            for (int k2=order[kplayer]; k2 != kplayer; k2 = order[k2]) sum += sums[k2];
            Simple.softAssert(sum==0);
        }
    }
    long running;
    int count, limit = 1<<14;
    int bseed;
    public void build() {
        new Db4j.Query() { public void task() throws Pausable {
            bseed = buildSeed.get(txn);
        } }.offer(db4j).awaitb();
        build(bseed);
        System.out.println("build loaded: " + bseed);
    }
    public void rotate(int numPasses,int numReps) {
        for (int ii = 0; ii < numPasses; ii++) {
            dorotate(numReps);
            System.out.format( "rotate completed %5d\n", ii );
            validate();
        }
    }
    public void dorotate(int numReps) {
        for (int ii = 0; ii < numReps; ii++)
            rot1(ii);
        conn.awaitb();
    }
    void swap1(int [] d1,int [] d2,int k1,int k2) {
        int delta = (d1[k1] - d2[k2])/2;
        d1[k1] -= delta;
        d2[k2] += delta;
    }
    int complex(int k1,int k2,int k3,double p3) {
        double pp = r1.rand();
        return (pp < p3) ? r1.rand(k1,k3) : r1.rand(k1,k2);
    }
    public void rot1(int kiter) {
        final int kplayer = r1.rand(0,np);
        final int nt = complex(0,5,12,.1);
        final int [] items = r1.rand(new int[nt+1],1,nv);
        new Db4j.Query() {
            public void task() throws Pausable {
                int [] data = new int[nv], d2;
                map.setdata(txn,kplayer*nv,data,new Command.RwInts(),nv);
                yield();
                for (int ii=0, victim=data[0]; ii < nt; ii++, victim=d2[0]) {
                    d2 = new int[nv];
                    if (victim==kplayer) victim = data[0];
                    map.setdata(txn,victim*nv,d2,new Command.RwInts(),nv);
                    yield();
                    swap1(data,d2,items[ii],items[ii+1]);
                    map.setdata(txn,victim*nv,d2,new Command.RwInts().init(true),nv);
                }
                map.setdata(txn,kplayer*nv,data,new Command.RwInts().init(true),nv);
            }
        }.offer(conn);
    }
    long [] sums = new long[np];
    long asum = 0, suma=0;
    void validate() {
        asum = 0;
        suma = 0;
        for (int ii = 0; ii < np; ii++) {
            final int kplayer = ii;
            new Db4j.Query() {
                public void task() throws Pausable {
                    int [] data = new int[nv];
                    map.setdata(txn,kplayer*nv,data,new Command.RwInts(),nv);
                    yield();
                    long sum = sums[kplayer] = sum(data);
                    asum += abs(data);
                    suma += Math.abs(sum);
                    Simple.softAssert(data[0]==order[kplayer],"validate: %d, %d != %d\n",
                            kplayer,data[0],order[kplayer]);
                }
            }.offer(conn);
        }
        conn.awaitb();
        long avg = Simple.Rounder.divup(asum,1L*np*nv);
        System.out.format("validate data load complete: %12d --> %5d, %5d\n", asum, avg, suma);
        validateSums();
    }
    public void close() {
        db4j.shutdown();
    }
    public static class Demo {
        public static void main(String [] args) {
            Simple.Scripts.cpufreqStash( 2300000 );
            DemoEntropy test = new DemoEntropy();
            if (false | args.length > 0 && "init".equals(args[0])) {
                test.doinit();
            }
            else {
                int numPasses = 10, numReps = test.nr;
                if (args.length > 0) numPasses = Integer.parseInt(args[0]);
                if (args.length > 1) numReps   = Integer.parseInt(args[1]);
                test.load();
                test.build();
                test.validate();
                test.rotate(numPasses,numReps);
            }
            test.close();
        }
    }
    
}
