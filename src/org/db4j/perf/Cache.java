// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import org.srlutils.Rand.XorShift;
import org.srlutils.Util;
import org.srlutils.data.BloomFilter;
import org.srlutils.data.GoldenSearch;
import org.srlutils.Timer;
import org.srlutils.data.Prob;

public class Cache {
    public static int cbits = 13;
    public static int size = 1 << cbits;

    /** lookup and freshen address, return true if found */
    public boolean lookup(long addr) { return false; }
    /** add the address to the cache - note: lookup(address)==false */
    public void complete() {}
    public String msg() { return ""; }


    static int signum(long v1,long v2) { return v1==v2 ? 0 : v1 > v2 ? 1 : -1; }
    static int signum(int v1,int v2) { return v1==v2 ? 0 : v1 > v2 ? 1 : -1; }

    public static class Addr implements Comparable<Addr> {
        public long addr;
        public boolean touch;
        public int count, age, flat;
        public Addr set(long $addr) { addr = $addr; return this; }
        public int compareTo(Addr o) { return signum(addr, o.addr); }
        public double devs(double uo) {
            double num = count - uo*age;
            return (num >= 0 ? 1.0 : -1.0)*num*num / (.1 + age);
        }
        public double rate() { return 1.0*count/age; }
        public String txt(double uo) { return String.format("%8d(%5d,%5d) --> %8.3f, %8.3f", addr, count, age, devs(uo), rate() ); }

        public static class CountSort implements Comparator<Addr> {
            public int compare(Addr o1, Addr o2) { return signum( o1.count, o2.count ); }
        }
        public static class DevsSort implements Comparator<Addr> {
            double uo = 2.0;
            public int compare(Addr o1, Addr o2) { return -Double.compare( o1.devs(uo), o2.devs(uo) ); }
        }
    }

    public static void dump(TreeSet<Addr> set) {
        for (Addr key : set) System.out.format( "%2d..%-5d ", key.addr, key.count );
        System.out.format( "\n" );
    }



    public static class Incremental extends Cache {
        public TreeSet<Addr> cache = new TreeSet();
        public TreeSet<Addr> broom = new TreeSet();
        public TreeSet<Addr> fifo = new TreeSet();
        public int nn = size/3, year = nn, today, decade, nhits, npromote;
        public Addr next, weakest;
        public double uo = 1.0;
        public double cumrnd = 0;

        public void sat(Addr mat) {
            int lim = 32;
            if (mat.count == lim || mat.age == lim) {
                int ao = mat.age, co = mat.count;
                mat.age *= .8;
                double c1 = 1.0*co*mat.age/ao;
                mat.count = (int) Math.round( c1 + .1*cumrnd );
                cumrnd += (c1-mat.count);
            }
        }


        public Addr [] sort() {
            Addr[] addrs = cache.toArray( new Addr[0] );
            Addr.DevsSort sorter = new Addr.DevsSort();
            sorter.uo = uo;
            Arrays.sort( addrs, sorter );
            return addrs;
        }

        public void filter() {
            Addr [] addrs = sort();
            int len = Math.min( addrs.length, 28 * nn / 10 );
            weakest = addrs[len-1];
            uo = addrs[(int)(len*.8)].rate();
            for (Addr addr : broom) fifo.add( addr );
            broom  = new TreeSet();
            cache = new TreeSet();
            for (int ii =   0; ii <          len; ii++) cache.add( addrs[ii] );
            for (int ii = len; ii < addrs.length; ii++)  fifo.add( addrs[ii] );
        }

        public Addr get(TreeSet<Addr> tree,Addr key,Addr ret) {
            Addr mat = tree.ceiling( key );
            if (mat != null && mat.addr == key.addr) {
                if (tree != cache) promote( mat, tree, cache );
                return mat;
            }
            else return ret;
        }

        public Addr next() {
            if (next==null) next = fifo.first();
            Addr tmp = next;
            next = fifo.higher( next );
            return tmp;
        }
        public void remove() { fifo.remove( next() ); }

        public void promote(Addr addr,TreeSet<Addr> src,TreeSet<Addr> dst) {
            if (dst==cache) npromote++;
            src.remove( addr );
            dst.add( addr );
            // maybe should rebalance fifo vs broom ???
        }

        public String msg() {
            return String.format( "%5d, %5d, %5d", cache.size(), broom.size(), fifo.size() );
        }
        public void info() {
            for (Addr addr : sort()) System.out.println( addr.txt(uo) );
        }

        public void reset() {
            for (Iterator<Addr> iter = cache.iterator(); iter.hasNext();) {
                Addr each = iter.next();
                each.age++;
                each.flat++;
                each.touch = false;
                sat(each);
            }
            System.out.format(
                    "decade: %5d, %5d days, %8.3f rate -- [%5d %5d %5d] cbf, %5d up",
                    decade, today, 1.0*nhits/today, cache.size(), broom.size(), fifo.size(), npromote
                    );
            filter();
            System.out.format( ", %8.3f devs, %8.3f uo, %8.3f weakest\n", weakest.devs(uo), uo, weakest.rate() );
            decade++;
            today = 0;
            nhits = 0;
            npromote = 0;
        }

        public boolean lookup(long addr) {
            today++;
            if (today % year == 0) {
                for (Addr each : cache) each.touch = false;
            }
            if (nhits >= size) reset();
            Addr key = new Addr().set( addr );
            Addr mat = get( cache, key, null );
            if (mat==null) mat = get( broom, key, null );
            if (mat==null) mat = get(  fifo, key, null );
            if (mat != null) {
                if (mat.touch == false) { mat.count++; mat.touch = true; sat(mat); }
                mat.flat = 0;
                nhits++;
                return true;
            }

            int nc = cache.size();
            int nb = broom.size();
            if (nb < (size-nc)/2) broom.add(key);
            else {
                if (nc+nb+fifo.size() >= size && ! fifo.isEmpty()) remove();
                if (nc+nb+fifo.size() <  size)                     fifo.add( key );
            }
            return false;
        }
    }

    public static class Cost extends GoldenSearch.Costable {
        public Prob.IntegerGamma [] gammas = new Prob.IntegerGamma[ 64 ];
        public int kg;
        public int ncalls;
        public TreeSet<Addr> tree;
        public int size;

        public Cost set(TreeSet<Addr> $tree,int $size) { tree = $tree; size = $size; return this; }

        public Prob.IntegerGamma get(double bn) {
            Prob.IntegerGamma gamma = null;
            int ii;
            for (ii = 0; ii < gammas.length; ii++) {
                gamma = gammas[ii];
                if (gamma != null && gamma.x == bn) { gammas[ii] = gammas[kg]; break; }
            }
            if (ii == gammas.length || true) {
                gamma = new Prob.IntegerGamma( bn, 256 );
//                    System.out.format( "gamma unfound: %8.3f\n", bn );
            }
            gammas[ kg++ ] = gamma;
            kg %= gammas.length;
            return gamma;
        }
        /**
         * calculate and return the max likelihood cost
         * assumes a ramp on [0,b)
         * cost: ln P(k1,k2,...,km|B=b) = sum( ln P(ki|b) )
         * automatically scale the intermediate results to maintain numerical stability
         */
        public double cost(double b) {
            ncalls++;
            double sat = 0.001;
            int min = -1000000;
            // note: saturate bn st > 0, and penalize ... needed to ensure unimodal
            double bn = Math.max( b*size, sat ), cost = 1;
            Prob.IntegerGamma ig = get( bn );
            int sum = 0;
            for (Addr key : tree) {
                int k = key.count;
                double gamma = 2*(k+1)*(k+2) / (bn*bn) * ig.eval(k+2);
                if ( gamma <= 0 || sum <= min || Double.isNaN( gamma ) ) {
                    sum = min;
                    break;
                }
                cost *= gamma;
                int exp = Math.getExponent( cost );
                sum += exp;
                cost = Math.scalb( cost, -exp );
            }
            if (sum <= min)
                return 2*min*Math.log(2) - Math.abs( tree.first().count - bn );
            cost = Math.log( cost ) + sum * Math.log(2);
//                if (bn == sat) cost -= (sat-b*broomsize);
//                System.out.format( "cost: %8.3f --> %8.3f\n", bn, cost );
            return cost;
        }

    }

    public static class Sweep extends Cache {
        public int qn = 2000*size, broomsize = size, today, tenure = 0;
        public TreeSet<Addr> cache = new TreeSet();
        public TreeSet<Addr> broom = new TreeSet();
        public Cost coster = new Cost().set( broom, broomsize );
        public double center = 2;

        public static Addr get(TreeSet<Addr> tree,Addr key,Addr ret) {
            Addr mat = tree.ceiling( key );
            return (mat != null && mat.addr == key.addr) ? mat : ret;
        }

        public double gridSearch() {
            Timer.timer.tic();
            double dx = 0.5 / broomsize;
            int nd = (int) Math.round( center / dx );
            double bo = nd * dx;
            bo = center;
            coster.verbose = false;
            coster.ncalls = 0;
            center = coster.search( 2*dx, bo-dx, bo+dx );
            return Timer.timer.tock();
        }

        public static void stats(TreeSet<Addr> tree,int min,int max) {
            int nn = tree.size(), step = 5, nb = (max - min) / step;
            int [] data = new int[ nb ];
            for (Addr val : tree) {
                int ko = Util.Scalar.bound( 0, nb-1, (val.count - min)/step );
                data[ ko ]++;
            }
            for (int ii = 0; ii < data.length; ii++) System.out.format( "stats: %5d %5d\n", ii*step+min, data[ii] * 1000 / nn );
        }

        public boolean lookup(long addr) {
            today++;
            Addr key = new Addr().set( addr );
            Addr mat = get( cache, key, null );
            if (mat != null) { mat.count++; return true; }
            mat = get( broom, key, null );
            if (cache.size() < size)               cache.add( key );
            else if ( mat != null )                  mat.count++;
            else if ( broom.size() < broomsize )   broom.add( key );

            if (today-1 > qn) {
                double time = gridSearch();
//                stats( broom, 0, 150 );
                Addr key511 = new Addr().set( 500 );
                key511 = get( cache, key511, key511 );
                int sum = 0;
                for (Addr val : cache) sum += val.count;
                System.out.format( "%8.4f bo: %8.3f %5d in %8.7f seconds -- ", 1.0*sum/qn, center*broomsize, coster.ncalls, time );
//                dump( cache );
                Addr [] cv = new Addr[ size + broomsize ];
                int ii = 0;
//                for (Addr val : cache) { cv[ii++] = val; val.count = (val.count+tenure) >> 1; }
                for (Addr val : cache) { cv[ii++] = val; val.count += tenure; }
                for (Addr val : broom)   cv[ii++] = val;
                Arrays.sort( cv, new Addr.CountSort() );
                cache.clear();
                broom.clear();
                for (ii = broomsize; ii < cv.length; ii++) cache.add( cv[ii] );
                System.out.format( "%5d --> %5d .. %5d\n", key511.count, cache.first().count, cache.last().count );
                for (Addr val : cache) val.count = 0;
                today = 0;
            }
            return false;
        }

    }

    public static class Bloom extends Cache {
        public BloomFilter pan, bin;
        public TreeSet<Key> addrset = new TreeSet();
        public TreeSet<Key> ageset = new TreeSet( new Ager() );
        public TreeSet<Long> broom;
        public int today, broomday, qn = 20*size, broomsize = size;
        public static double decay = .5;


        public static class Ager implements Comparator<Key> {

            public int compare(Key o1, Key o2) {
                int c1 = o1.composite(), c2 = o2.composite();
                if (c1 == c2) return signum( o1.addr, o2.addr );
                else return signum( c1, c2 );
            }
        }

        public static class Key implements Comparable<Key> {
            public long addr;
            public int period, birth;
            public Key(long $addr) { addr = $addr; }
            public Key(long $addr,int $period,int $birth) { addr = $addr; period = $period; birth = $birth; }
            public int compareTo(Key o) { return signum(addr, o.addr); }
            public int calc(int today) {
                int delta = today - birth;
                return (int) (decay * period + (1-decay) * delta);
            }
            public void update(int today) {
                period = calc( today );
                birth = today;
            }
            public int periodHat(int today) {
                int delta = today - birth;
                if (false) return Math.max( delta, period );
                return (delta <= period)
                        ? period
                        : (int) (decay * period + (1-decay) * delta);
            }
            public int composite() { return period - birth/2; }
            public Key set(int today,int broomday) { birth = today; period = today - broomday; return this; }
            public String info(int today) { return String.format( "%2d %5d %5d", addr, period, today-birth ); }
        }

        public void next() {
            broom = new TreeSet();
            broomday = today;
        }

        public Bloom() {
            next();
            pan = new BloomFilter().init( size, 0.05 );
        }

        public void add(Key key) {
            addrset.add( key );
            ageset.add( key );
        }
        public void remove(Key key) {
            addrset.remove( key );
            ageset.remove( key );
        }
        public void dump() {
            for (Key key : addrset) System.out.format( "%2d ", key.addr );
            System.out.format( " -- \n" );
            for (Key key : ageset) System.out.format( "%2d %5d %5d\n", key.addr, key.period, today - key.birth );
            System.out.format( "\n" );
        }

        public boolean lookup(long addr) {
            today++;
            Key key = new Key( addr );
            Key match = addrset.ceiling( key );
            if (match != null && match.addr == addr) {
                ageset.remove( match );
                match.update( today );
                ageset.add( match );
                return true;
            }
            if (ageset.size() < size) {
                key.set( today, today - qn );
                add( key );
                return false;
            }
            boolean found = (broom.size() < broomsize)
                    ? broom.     add( addr )
                    : broom.contains( addr );
            if (found) {
                key.set( today, broomday );
                Key first = ageset.last();
                int period = first.periodHat( today );
                if (period > key.period) {
                    System.out.format( "replace: %s --> %s\n", first.info(today), key.info(today) );
                    remove( first );
                    add( key );
                }
            }
            if (today - broomday > qn) {
                dump();
                next();
            }
            return false;
        }

    }




    public static class Jets extends Cache {
        TreeSet set = new TreeSet();
        public boolean lookup(long addr) {
            boolean found = set.contains( addr );
            if (!found && set.size() < size) set.add( addr );
            return found;
        }
    }

    public static class Synth {
        public XorShift source = new XorShift();
        public int bits = cbits + 5;
        public long nn = 1 << bits;
        public int next2() { return (int) source.next( bits ); }
        public int next3() {
            long val = source.next( 2*bits );
            int log2 = 64 - Long.numberOfLeadingZeros( val-1 );
            int shift = log2 >> 1;
            return (int) (val >>> shift);
        }
        public int next() {
            long val = source.next( 2*bits );
            return (int) Math.sqrt( val );
        }

        public void test(Cache cache) {
            int nt = 1 << 27, pg = size << 6;
            int nf = 0;
            int dd = ( 1 << (bits - cbits) );
            double nominal = 1.0 / dd;
            double adjusted = 1.0 * (dd*dd - (dd-1)*(dd-1)) / dd / dd;
            source.seed(org.srlutils.Rand.irand() );
            source.seed( 7 );
            for (int ii = 0; ii < nt; ) {
                for (int jj = 0; jj < pg; jj++) {
                    int key = next();
                    boolean found = cache.lookup( key );
                    if (found) nf++;
                }
                cache.complete();
                ii += pg;
                if (ii >= nt) System.out.format(
                        "%5d -- found: %d, rate: %8.3f, adjusted: %8.3f, nominal: %8.3f -- %s\n",
                        ii/pg, nf, 1.0*nf/ii, adjusted, nominal, cache.msg()
                        );
            }
        }

    }

    public void doit() {
        Incremental cache = new Incremental();
        Synth synth = new Synth();
        synth.test( cache );
    }

    public static void main(String [] args) {
        Incremental cache = new Incremental();
        Synth synth = new Synth();
        synth.test( cache );
        cache.info();
    }


}
