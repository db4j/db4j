// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import com.nqzero.directio.DioNative;
import java.io.File;
import kilim.Pausable;
import org.db4j.Command;
import org.db4j.Db4j;
import org.db4j.Db4j.Query;
import org.db4j.Db4j.Transaction;
import org.db4j.HunkArray;
import org.srlutils.Simple;
import org.srlutils.Stats;
import org.srlutils.Timer;
import org.srlutils.Util;
import org.srlutils.hash.LongHash;

public class DemoHunker {
    static boolean async = true;
    static boolean useSort = false;
    static final String PATH_BASE = "///db4j/demohunker/store_";

    public static class DemoRollback extends DemoWithTasks {
        public int [] counts;
        public void prep() { counts = new int[nn]; }
        public volatile int dummy = 0;
        public int nback = 0;
        static boolean firstCount = true;
        

        public class Count extends BaseKask {
            boolean post;
            public void task() throws Pausable {
                {
                    cmd = arrays[0].get( tid, offsets[ii] );
                    yield();
                }
                {
                    long co = cmd.val;
                    boolean valid = co >= 0 && co < nn;
                    if (co!=kv[ii]) {
                        System.out.format( "drb.mismatch: %5d %5d %5d --> %5d\n", ii, co, kv[ii], offsets[ii] );
                    }
                    if (valid) counts[ (int) co ]++;
                    if (valid & counts[(int) co] > 1)
                        System.out.format( "drb.over: %5d %5d --> %8d %8d\n",
                                ii, co, offsets[ii], offsets[(int)co] );
                    post = true;
                }
            }
            public boolean postRun(boolean pre) {
                Exception ex = getEx();
                if (!post | ex != null) {
                    System.out.format("count:post - %b %s\n",post,ex);
                    if (ex != null && firstCount) {
                        ex.printStackTrace();
                        firstCount = false;
                    }
                }
                done++;
                return false;
            }
            public String report() {
                boolean d2 = dbg || true;
                String txt = d2 ? "\n" : ", ";
                int max = Util.maxi( counts );
                Stats.FreqData fd = new Stats.FreqData().occur( counts );
                if (d2) for (Stats.Freq freq : fd.get())
                    txt += String.format( "\tRollback -- occur:%5d, count:%5d\n", freq.val, freq.count );
                txt += String.format( "Max occur: %d x %d, done:%d", max, counts[max], done );
                return txt;
            }
            public boolean success() { int[] mm = Util.bounds(counts); return mm[0]==1 && mm[1]==1; }
        }
        
        public class Rollback extends BaseKask {
            public long k1, k2;
            public int i1 = source.nextInt( nn );
            public int i2 = source.nextInt( nn );
            int ni = (source.nextInt(100)==0) ? 32:2;
            int [] io = source.rand(new int[ni],0,nn);
            long [] ko = new long[ni];
            

            public void rollback(Db4j db4j,boolean restart) {
                nback++;
                if (dbg) System.out.format( "demo.roll -- %5d %5d --> %5d %5d\n", i1, i2, k1, k2 );
                super.rollback(db4j,restart);
            }
            public String report() {
                return String.format( ", nback: %5d, nwait: %5d\n", nback, db4j.guts.stats2() );
            }

            public boolean postRun(boolean pre) {
                boolean mis = false;
                for (int jj=0; jj < ni/2; jj++) {
                    int jo = ni-1-jj;
                    int ix = io[jj];
                    int iy = io[jo];
                    if (kv[ix] != ko[jj] | kv[iy] != ko[jo]) mis = false;
                    int kx = kv[ix];
                    kv[ix] = kv[iy];
                    kv[iy] = kx;
                }
                if (dbg | mis) {
                    for (int ix : io)
                        System.out.format("%5d  ",ix);
                    System.out.format(" -- ");
                    for (int ix : io)
                        System.out.format("%5d  ",kv[ix]);
                    if (mis) {
                        System.out.format(" == ");
                        for (int jj=0; jj < ni; jj++)
                            System.out.format("%5d  ",ko[jj]);
                    }
                    System.out.format("\n");
                }
                done++;
                if (mis)
                    Simple.softAssert(false);
                return false;
            }
            
            public void task() throws Pausable {
                for (int jj=0; jj < ni/2; jj++) {
                    int jo = ni-1-jj;
                    int ix = io[jj];
                    int iy = io[jo];
                    cmd = arrays[0].get( tid, offsets[ix] );
                    yield();
                    ko[jj] = cmd.val;
                    cmd = arrays[0].get( tid, offsets[iy] );
                    yield();
                    ko[jo] = cmd.val;
                    arrays[0].set( tid, offsets[ix], ko[jo] );
                    arrays[0].set( tid, offsets[iy], ko[jj] );
                }
            }
        }


    }

    
    public static class DemoInfo extends Demo<DemoInfo> {
        public void test() {
            new Query() { public void task() throws Pausable {
                arrays[0].printMetaHunkInfo(tid);
            }}.offer(db4j).awaitb();
        }
    }

    public static class DemoWithTasks extends Demo<DemoWithTasks> {
        public int done, wrong, delta = 7;
        public Class<? extends Taskable> taskKlass;
        public DemoWithTasks set(Class<? extends Taskable> klass) { taskKlass = klass; return this; }

        public static Aliases aliases = new Aliases();
        public static class Aliases {
            public Class< CopyTask>             copy  =              CopyTask.class;
            public Class<WriteTask>             write =             WriteTask.class;
            public Class< ReadTask>             read  =              ReadTask.class;
            public Class<CheckTask>             check =             CheckTask.class;
            public Class<DemoRollback.Rollback> roll  = DemoRollback.Rollback.class;
            public Class<DemoRollback.Count   > count = DemoRollback.Count   .class;
        }
        

        public interface Taskable {
            String report();
            Query set(int $ii,long [] $offsets,int [] $kv);
            boolean success();
        }
        public void report(double tc,String report) {
            int ios = db4j.guts.stats();
            System.out.format( "Demo::%-10s - %5.2fs --> %5.1f iops, %5.2f iopt%s\n",
                    taskKlass.getSimpleName(), tc, 1.0*ios/tc, 1.0*ios/nn, report );
        }
        
        public abstract class BaseKask extends Query implements Taskable {
            public int ii;
            public Command.RwLong cmd;
            public long [] offsets;
            public int [] kv;
            public BaseKask set(int $ii,long [] $offsets,int [] $kv) {
                ii = $ii;
                offsets = $offsets;
                kv = $kv;
                return this;
            }
            public String report() { return ""; }
            public boolean success() { return true; }
            public boolean postRun(boolean pre) {
                super.postRun(pre);
                done++;
                return false;
            }
        }
        public class CopyTask extends BaseKask {
            public void task() throws Pausable {
                if (dbg) System.out.format( "CopyTask::task -- %5d, %5d, %8d\n", ii, 0, offsets[ii] );
                cmd = arrays[0].get( tid, offsets[ii] ); yield();
                cmd = arrays[1].set( tid, offsets[ii], cmd.val+delta ); yield();
                if (dbg) System.out.format( "CopyTask::done -- wrote to location %d\n", ii );
            }
        }
        public class WriteTask extends BaseKask {
            public void task() throws Pausable {
                cmd = arrays[0].set( tid, offsets[ii], (long) (ii) );
            }
        }
        public class ReadTask extends BaseKask {
            public void task() throws Pausable {
                cmd = arrays[0].get( tid, offsets[ii] );
                yield();
                wrong += cmd.val;
                done++;
            }
            public String report() { return String.format( ", sum of reads: %5d", wrong ); }
        }
        public class CheckTask extends BaseKask {
            public void task() throws Pausable {
                cmd = arrays[1].get( tid, offsets[ii] );
                yield();
                int val = (int) cmd.val;
                wrong += check( tid, val, ii+delta, val-delta, ii, 1 );
                done++;
            }
            public String report() { return String.format( ", %d unmatched, %8.3f %%", wrong, 1.0*wrong/nn ); }
            public boolean success() { return wrong==0; }
        }
        public boolean success() { return Simple.Reflect.newInner( taskKlass, this ).success(); }

        public void test() {
            Taskable task = null;
            timer.tic();
            for (int ii = 0; ii < niter; ii++) {
                task = Simple.Reflect.newInner( taskKlass, this );
                Query t2;
                db4j.submitQuery( t2 = task.set(ii,offsets,kv) );
                if (! async) t2.awaitb();
            }
            while (done < niter) Simple.sleep(100);
            db4j.guts.sync();
            double tc = timer.tock();
            report( tc, task.report() );
            db4j.guts.dontneed();
            if (taskKlass==WriteTask.class && false)
                new Query() { public void task() throws Pausable {
                    arrays[0].printMetaHunkInfo(tid);
                }}.offer(db4j).awaitb();
        }
    }

    
    public static abstract class Demo<TT> implements Cloneable {
        public Db4j db4j;
        public HunkArray.L [] arrays;
        public org.srlutils.rand.Source source = new org.srlutils.rand.Source();
        public int nstores, nn, niter;
        public Timer timer = new Timer();
        public boolean drop = false;
        public long size, seed, offsets[];
        public int kv[];
        public boolean dbg = false;
        public boolean force = false;
        
        public void cleanup() {
            db4j = null;
            arrays = null;
        }

        public void make() {
            Simple.softAssert( nn <= size );
            // make a hash table large enough to store all the offsets to avoid duplicates
            int nbits = 32-Integer.numberOfLeadingZeros(nn-1);
            LongHash.Set table = new LongHash.Set().init(nbits+1);
            offsets = new long[nn];
            int ndup = 0;
            for (int ii = 0; ii < nn; ii++) {
                long val;
                while (table.contains( val = source.rand(0L,size) )) ndup++;
                table.put(val);
                offsets[ii] = val;
            }
            if (useSort) java.util.Arrays.sort(offsets);
            kv = Util.colon(nn);
        }
        
        public void prep() {}
        public TT prep(int $nn,long $seed) {
            niter = nn = $nn;
            seed = $seed;
            source.setSeed( seed );
            make();
            prep();
            return (TT) this;
        }

        public TT dup() {
            try { return (TT) clone(); } catch (Exception ex) {}
            return null;
        }

        public TT init(boolean $force,long $size,int $nstores) {
            force = $force;
            nstores = $nstores;
            size = $size;
            return (TT) this;
        }
        public void start() {
            String name = mapFilename;
            //            name = "/dev/sdb2";
            db4j = new Db4j();
            File file = new File( name );
            if ( ! file.exists() || force ) {
                // don't auto delete the file ... it takes forever to recreate it (limit of linux/ext3)
                //                file.delete();
                long fileSize = (size*8 + size*8/128)*nstores + (1<<20) + (1<<30);
                db4j.init( name, fileSize );
                arrays = new HunkArray.L[ nstores ];
                for (int ii = 0; ii < nstores; ii++)
                    arrays[ii] = db4j.register(new HunkArray.L(),PATH_BASE + ii);
                db4j.create();
            }
            else {
                db4j = Db4j.load( name );
                arrays = new HunkArray.L[ nstores ];

                db4j.submitCall(tid -> {
                    for (int ii=0; ii < nstores; ii++)
                        arrays[ii] = (HunkArray.L) db4j.lookup(tid,PATH_BASE + ii);
                }).awaitb();
            }
        }
        public TT run() {
//            hunker.new Doctor().start();
            try {
                start();
                if (drop) DioNative.dropCache();
                test();
                if (dbg) db4j.guts.info();
//                Simple.sleep(1000);
//                hunker.forceCommit(10);
                db4j.shutdown();
            }
            catch (Exception ex) { throw Simple.Exceptions.rte(ex); }
            return (TT) this;
        }
        public abstract void test();
        public int check(Transaction tid,int val,int goal,int alt,int ii,int jj) throws Pausable {
            long offset2 = (alt < 0 || alt >= offsets.length) ? -1 : offsets[alt];
            if ( val != goal && offsets[ii] != offset2 ) {
                if ( Db4j.debug.test ) System.out.format(
                            "testRead: %5d, %8d --> %8d --> %8d v %8d -- %s\n",
                            jj, ii, val, offsets[ii], offset2,
                            arrays[jj].offsetInfo( tid, offsets[ii] ) );
                return 1;
            }
            return 0;
        }
    }
    public static String mapFilename;

    static String parse(String arg,String prefix) {
        if (arg.startsWith(prefix)) return arg.substring(prefix.length());
        return null;
    }

    /**
     * some automated tools execute maven projects from the target directory which complicates demos.
     * if name is a relative path, target is the current directory and the file doesn't exist in it,
     * and the file exists in the parent directory, use that file name and print a warning.
     * otherwise return the name unchanged
     * @param name the filename
     * @return the resolved filename
     */
    public static String resolve(String name) {
        File file = new File(name);
        if (! file.exists() & ! file.isAbsolute()) {
            File dir = new File("");
            String dirname = dir.getAbsoluteFile().getName();
            String newname = "../" + name;
            if ("target".equals(dirname) & new File(newname).exists()) {
                System.out.println(
                        "db4j.DemoHunker.resolve: using database filename for parent directory, ie " + newname);
                return newname;
            }
        }
        return name;
    }
    
    public static void main(String [] args) throws Exception {
        boolean write = false, copy = false, rock = false, read = false, check = false, count = false;
        final int nstores = 2;
        int nn = (1 << 14) / nstores;
        long size = (9L << 28) / nstores;
        Long seed = null;
        String mode = "wkn";
        String filename = null;

        String val;
        for (String arg:args) {
            if      ((val = parse(arg,"name:")) != null) filename    = val;
            else if ((val = parse(arg,"num:"))  != null) nn          = Integer.parseInt(val);
            else if ((val = parse(arg,"seed:")) != null) seed        = Long.parseLong(val);
            else if ((val = parse(arg,"mode:")) != null) mode        = val;
        }
        mapFilename = filename==null ? resolve("./db_files/b6.mmap") : filename;

        for (char cc:mode.toCharArray()) {
            if (cc=='w') write = true;
            if (cc=='y') copy = true;
            if (cc=='k') rock = true;
            if (cc=='r') read = true;
            if (cc=='c') check = true;
            if (cc=='n') count = true;
        }

        if (seed==null) write = true;
        seed = org.srlutils.Rand.source.setSeed( seed, true );

        //  flags are final now - need to set manually
        //        Hunker.debug.cache = true;
        
        DemoWithTasks xx, x1, x2, x3, w1;
        x1 = x2 = x3 = new DemoWithTasks().set( DemoWithTasks.WriteTask.class );

        DemoWithTasks.Aliases aa = DemoWithTasks.aliases;
        DemoWithTasks ro = new DemoWithTasks().init(false,size,nstores).prep(nn,seed);
        DemoWithTasks rw = new DemoWithTasks().init( true,size,nstores).prep(nn,seed);
        final DemoRollback  rb = new DemoRollback();
        rb.init(false,size,nstores).prep(nn,seed);

        final DemoRollback  r1 = new DemoRollback();
        r1.init(false,size,nstores).prep(nn,seed);
        final DemoRollback  r2 = new DemoRollback();
        r2.init(false,size,nstores).prep(nn,seed);
        
        
        if (write) {
            w1 = rw.dup().set(aa.write).run();
//            DemoWithTasks y2 = r2.dup().set(aa.count).run();
//            Simple.softAssert( y2.success(), "initial write not verified - y2" );
        }
//        DemoWithTasks y1 = r1.dup().set(aa.count).run();
//        Simple.softAssert( y1.success(), "initial write not verified - y1" );
        if (copy)
            xx = ro.dup().set(aa.copy).run();
        if (check)
            x1 = ro.dup().set(aa.check).run();
        if (count && copy)
            x2 = ro.dup().set(aa.count).run();
        if (rock) {
            xx = rb.dup().set(aa.roll);
            xx.niter = nn;
            xx.run();
        }
        if (count)
            x3 = rb.dup().set(aa.count).run();
        if (read)
            xx = ro.dup().set(aa.read).run();
        
        if (x1.success() && x2.success() && x3.success()) System.out.format( "Success\n" );
        else System.out.format( "Failure\n" );
                
        
    }
    
}

/*
 * unsorted reads (ie count) without rock ... iops vs maxtasks (in thousands) in a 2G file
 *     at low densities you get 100-ish iops
 *     at high densities you get 100mbps-ish, ie 25k iops
 *     shows that at low densities there's not a huge gain from increasing io density
 *     but at higher densities it's almost linear
 *   octave:32> x=[0.001 .5 1 2 4 8 16 32]; y=[100 350 375 400 490 780 1400 2800];
 *   octave:33> plot(x,y,'o-'); hold on; plot([0 32],[0 2800],'r:'); hold off;
 * 
 * unsorted write+count, testing fallocate vs dd initialized hunk.mmap files
 *     nqzero.chowder.DemoHunker ./db_files/hunk3.mmap 1000000
 *       dd              : WriteTask  - 4155.99s --> 474.4 iops,  1.97 iopt
 *       fallocate pass 1: WriteTask  - 2676.39s --> 736.1 iops,  1.97 iopt
 *       fallocate pass 2: WriteTask  - 3812.98s --> 516.7 iops,  1.97 iopt
 *       dd              : Count      - 2195.76s --> 443.3 iops,  0.97 iopt
 *       fallocate pass 1: Count      - 2183.18s --> 445.7 iops,  0.97 iopt
 *       fallocate pass 2: Count      - 2189.74s --> 444.4 iops,  0.97 iopt
 *   note: initially writes are faster with fallocate because blocks don't need to be read (ie, partial writes)
 *         as the pages get written, performance should approach dd
 *   caveats: didn't test rock
 *   conclusion: no practical difference between fallocate (which is much faster) and dd
 *               ie, use fallocate
 *
 *
 */