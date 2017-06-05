// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import java.text.DecimalFormat;
import java.util.Arrays;
import org.apache.commons.lang3.RandomStringUtils;
import org.srlutils.DynArray;
import org.srlutils.Simple;
import org.srlutils.Util;
import org.srlutils.btree.Bstring;
import org.srlutils.btree.Btypes;
import kilim.Pausable;
import org.db4j.Bmeta;
import org.db4j.Btree;
import org.db4j.Db4j;
import org.db4j.Db4j.Transaction;

public abstract class TestString<CC extends Bmeta.Context<?,?,CC>> {
    static final String PATH = TestString.class.getName();

    public static class SI extends Bmeta<SI.Data,String,Integer,Bstring.ValsString> {
        { setup(new Bstring.ValsString(),new Btypes.ValsInt()); }
        public Data context() { return new Data(); }
        public static class Data extends Bmeta.Context<String,Integer,Data> {
            public Data set(String  key)            { return set(key,-1); }
            public Data set(String $key,int $val) { key = $key; val = $val; return this; }
            public int val() { return match ? val : -1; }
            public String format(int both) {
                String txt = (key==null) ? "" : key;
                if (txt.length() > 30) txt = txt.substring(0,30);
                if (both==0) 
                    return String.format( "%-30s", txt);
                return String.format( "%-30s --> %5d", txt, val);
            }
        }
    }
    public static class LS extends Bmeta<LS.Data,Long,String,Btypes.ValsLong> {
        { setup(new Btypes.ValsLong(),new Bstring.ValsString()); }
        public Data context() { return new Data(); }
        public static class Data extends Bmeta.Context<Long,String,Data> {
            public Data set(long  key)            { return set(key,""); }
            public Data set(long $key,String $val) { key = $key; val = $val; return this; }

            public String format(int both) {
                String txt = (val==null) ? "" : val;
                if (txt.length() > 30) txt = txt.substring(0,30);
                if (both==0) 
                    return String.format("%8d"          , key);
                return     String.format("%8d --> %-30s", key, txt);
            }
        }
    }
    
    Bmeta<CC,?,?,?> map;
    CC cc;
    org.srlutils.rand.Source r1 = new org.srlutils.rand.Source(), r2 = new org.srlutils.rand.Source();
    { 
        Long seed = null;
        seed = -1958679775570534405L;
        r1.setSeed(seed,true);
    }
    DynArray.ints kdels = new DynArray.ints();
    DynArray.longs keys = new DynArray.longs();
    int target, num, decade = 1<<10;
    double limit = .999;

    public int delta() {
        int tb = 1<<this.tb;
        int delta = (int) Util.Scalar.bound(-limit*tb, limit*tb, 1.0*(target-num)/target*tb );
        return delta;
    }
    
    // 0:look, 1:verify, 2:insert, 3:remove
    public int type(long magic) {
        int type = (int) ((magic & mtype) >>> nseed+nlen);
        int delta = delta();
        if (type < thresh[0])       return 0;
        if (type < thresh[1])       return 1;
        if (type < thresh[2]+delta) return 2;
        return 3;
    }
    public long seed(long magic) { return magic & mseed; }
    public int length(long magic) {
        long len = magic & mlen >>> nseed;
        return min + (int) (len % max);
    }

    int charType = 2;
    int max = 256, min = 4;
    int nseed = 32, nlen = 16, ntype = 16; // low to high
    long mseed = ((1L<<nseed)-1);
    long  mlen = ((1L<<nlen )-1) << nseed;
    long mtype = ((1L<<ntype)-1) << nseed+nlen;
    int tb = ntype-2;
    int [] thresh = new int[] { 1<<tb, 2<<tb, 3<<tb };
    DynArray.chars alphabet = new DynArray.chars();
    {
        for (char ii = 'a'; ii <= 'z'; ii++) alphabet.add(ii);
        for (char ii = 'α'; ii <= 'ω'; ii++) alphabet.add(ii);
        for (char ii = 'ᵃ'; ii <= 'ᵛ'; ii++) alphabet.add(ii);
        for (char ii = '⒜'; ii <= '⒵'; ii++) alphabet.add(ii);
        for (char ii = '耀'; ii <= '耲'; ii++) alphabet.add(ii);
    }

    /** generate and return a random string from key */
    public String sprout(long key) {
        int len = (int) ((key & mlen) >>> nseed);
        long seed = key & mseed;
        if (charType==2) {
            r2.setSeed(seed);
            return RandomStringUtils.random(len,0,0,false,false,null,r2.prng);
        }
        char [] chars;
        if (charType==1) {
            int [] kv = r2.setSeed(seed).rand(new int[len],0,alphabet.size);
            chars = Util.select(alphabet.vo,kv);
        }
        else
            chars = r2.setSeed(seed).rand(new char[len],'a','z');
        String string = new String(chars);
        return string;
    }
    /**  return a key consisting of a seed and a non-zero length */
    public long key(long magic) {
        long len = length(magic);
        long seed = seed(magic);
        long key = len << nseed | seed;
        return key;
    }
    boolean force = false;
    public void predel(int index) { keys.set(index,-2); }
    public void undel(int index,long stored) { keys.set(index,stored); }
    public void del(int index) {
        long key = get(index);
        if (useTotal) totalBytes -= sprout(key).getBytes().length;
        kdels.add(index);
        keys.set(index,0);
        num--;
        verify();
    }
    public long get(int index) { return keys.get(index); }
    boolean useTotal = false;
    int totalBytes = 0;
    /** add key to the stored keys array, returning the index */
    public int preadd(long key) {
        int nd = kdels.size;
        int index = (nd==0) ? num : kdels.vo[--kdels.size];
        Simple.softAssert(index <= keys.size);
        keys.set(index,-1L);
        num++;
        verify();
        if (useTotal) totalBytes += sprout(key).getBytes().length;
        return index;
    }
    public void unadd(int index) {
        kdels.add(index);
        keys.set(index,0L);
        num--;
        verify();
    }
    public void add(int index,long key) {
        keys.set(index,key);
    }
    void verify() {
        int n1 = keys.size;
        int n2 = num;
        int n3 = kdels.size;
        Simple.softAssert(n1==n2+n3);
    }
    /**
     * convert the seed portion of magic to an index into the stored keys array
     * if magic is an index+1, will return the next key in the set
     * ie, it acts like an iterator
     */
    public int index(long magic) {
        if (num==0) return -1;
        long seed = magic & ~mtype;
        int size = num + kdels.size;
        int ko = (int) (seed % size);
        int jj = 0;
        for (; keys.vo[ko] <= 0 & jj < num; jj++)
            if (++ko==keys.size) ko = 0;
        int index = jj < num ? ko : -1;
        if (index==0)
            Simple.nop();
        return index;
    }
    /** choose a "random" index to the stored keys and return the path in the map to that key */
    abstract Btree.Path getPath(CC cc,int index,long key) throws Pausable;

    org.srlutils.Timer timer = new org.srlutils.Timer();
    int [] counts = new int[4];
    public int timedLoop(int size,int jj,int jo,int nn) {
        target = size;
        Arrays.fill(counts,0);
        timer.start();
        long magic = 0;
        int zeros = 0;
        for (int ii = 0; ii < nn; ii++, jo++) {
            magic = process(jo);
            if (magic==0) zeros++;
        }
        double time = timer.tock();
//        check(jo);
        int [] info = map.getInfo();
        double ratio = 100.0*num/Math.max(0,1);
        DecimalFormat formatter = new DecimalFormat("0.00E0");
        int nb = 2*(totalBytes + num*7);
        String total = useTotal ? formatter.format(nb) + String.format(" %3d",nb/num) : "na";
        String sizestr = formatter.format(size) + "/" + formatter.format(num);
        if (info==null) info = new int[3];
        System.out.format(
                "%5d -- %s delta: %6d time:%8.3f -- %5.1f%%, %3d %5d %3d -- %5.1f%%, %s -- %d\n",
                jj, sizestr, delta(), time, 100.0*counts[2]/nn, info[0], info[1], info[2], ratio, total, zeros);
        return jo;
    }
    public void randomWalk(int operationsPerPass,int maxSize,int numPasses) {
        init();
        int jj=0, jo=0;
        for (jj=0; jj < numPasses; jj++) {
            int size = r1.nextInt(0,maxSize);
            jo = timedLoop(size,jj,jo,operationsPerPass);
        }
        db4j.shutdown();
    }
    public abstract void check(CC cc,long key1,String sprout1);
    public static class Key extends TestString<SI.Data> {
        { map = new SI(); }
        Btree.Path getPath(SI.Data cc,int index,long key) throws Pausable {
            String sprout = sprout(key);
            cc.mode = Btree.modes.gte;
            Btree.Path path = map.findPath(cc.set(sprout),true);
            Btree.OpaquePage p0 = path.getPage();
            int next = 0;
            while (true) {
                check(cc,key,sprout);
                if (cc.val==index) return path;
                path = map.next(path,cc);
                if (path.isEqual(p0))
                    Simple.softAssert(false,"key was not found, don't think this should ever happen");
                map.getPath(path,cc);
                next++;
            }
        }
        /** verify that the context matches key1 */
        public void check(SI.Data cc,long key1,String sprout1) {
            Simple.softAssert(cc.match);
            int found = cc.val;
            long key2 = get(found);
            if (key2 != key1) {
                String sprout2 = sprout(key2);
                Simple.softAssert(sprout1.equals(sprout2));
            }
        }
        SI.Data ccset(SI.Data cc,long key,int index) { return cc.set(sprout(key),index); }
    }
    public static class Val extends TestString<LS.Data> {
        { map = new LS(); }
        Btree.Path getPath(LS.Data cc,int index,long key) throws Pausable {
            String sprout = sprout(key);
            cc.mode = Btree.modes.gte;
            Btree.Path path = map.findPath(cc.set(key),true);
            check(cc,key,sprout);
            return path;
        }
        public void check(LS.Data cc,long key1,String sprout1) {
            Simple.softAssert(cc.match);
            String sprout2 = cc.val;
            Simple.softAssert(sprout1.equals(sprout2));
        }
        LS.Data ccset(LS.Data cc,long key,int index) { return cc.set(key,index >= 0 ? sprout(key) : ""); }
    }
    abstract CC ccset(CC cc,long key,int index);
    String filename = DemoHunker.resolve("./db_files/b6.mmap");
    Db4j db4j;
    public void init() {
        db4j = new Db4j().init( filename, null ); // 1L << 32 );
        db4j.create();
        db4j.submit(txn -> db4j.create(txn, map, PATH)).awaitb();
        db4j.guts.forceCommit(100);
    }
    public long process(final int jo) {
        final long magic = r1.nextLong();

        final int type = type(magic);
        counts[type]++;
        // look verify insert remove
        class Task extends Db4j.Query {
            int index;
            long key, stored;
            // the keys manipulation (add,del) isn't thread safe
            // but querunner is (currently) single threaded
            // and none of the manipulations are Pausable, so they're effectively atomic
            public void task() throws Pausable {
                CC c2 = map.context().set(txn);
                key = key(magic);
                index   = (type==0) ? 0
                        : (type==2) ? preadd(key)
                        : index(magic);
                if (index < 0) return;
                if (type==0) {
                    String sprout = sprout(key);
                    c2.mode = Btree.modes.eq;
                    map.findData(ccset(c2,key,-1));
                    if (c2.match)
                        check(c2,key,sprout);
                }
                else if (type==1)
                    getPath(c2,index,get(index));
                else if (type==2) {
                    txn.addRollbacker(new Roller());
                    c2.mode = Btree.modes.gt;
                    map.insert(ccset(c2,key,index));
                }
                else if (type==3) {
                    txn.addRollbacker(new Roller());
                    stored = get(index);
                    predel(index);
                    Btree.Path path = getPath(c2,index,stored);
                    map.remove(path,c2);
                }
            }
            // fixme:correctness -- on rollback, need to undo preadd, predel
            public boolean postRun(boolean pre) {
                super.postRun(pre);
                if (type==2) add(index,key);
                if (type==3) del(index);
                return false;
            }
            class Roller extends Db4j.Rollbacker {
                public void runRollback(Transaction txn) {
                    if (type==2) unadd(index);
                    if (type==3) undel(index,stored);
                }
            }
            
        };
        new Task().offer(db4j);
        return magic;
    }
    

    public static class Demo {
        public static void main(String [] args) {
            Simple.Scripts.cpufreqStash( 2300000 );
            TestString test = new Val();
            int num = 19;
            test.randomWalk(1<<num,1<<num,128);
         }
    }
}

/*
 * using num=19, this ran overnight till iteration 77-ish, eventually bombing out when the file overflowed
 *   as hunks are not recycled when they're freed
 */


/*
 *  general scheme is have a random source r1 --> metakeys
 *  each metakey --> an action: put, look or remove, a seed and a length
 *  a second random source r2, r2.seed(metakey.seed)
 *  for puts:
 *    r2 --> data, a new random char[length]
 *    insert (data,keys.length) tuple into the map and append metakey to stored keys array
 *  for looks:
 *    map to index into keys, then use that metakey to regenerate and data and lookup in map
 *  for removes:
 *    map index into keys, use metakey to regenerate data and remove from map
 *    zero out entry in the key array and add the loc to the array of deleted keys
 *      which are recycled before 
 * 
 * 
 */
