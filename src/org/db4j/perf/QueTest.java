// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.srlutils.Simple;
import org.srlutils.Timer;

public class QueTest {
    ConcurrentLinkedQueue q = new ConcurrentLinkedQueue();
    public final AtomicInteger found = new AtomicInteger();
    public Object msg = new Object();
    public boolean check = true;
    public int mod = 0;

    public class Reader extends Thread {
        public void run() {
            int cnt = 0;
            while (true) {
                Object obj = q.poll();
                if (obj==null) Simple.sleep(0);
                else if (obj==msg) break;
                else cnt++;
                if (check && cnt > mod) { found.addAndGet( -cnt ); cnt = 0; }
            }
            if (check) found.addAndGet( -cnt );
        }
    }

    public void write() {
        Timer timer = new Timer();
        int nb = (1<<22), np = 16;
        int cnt = 0;
        for (int ii = 0; ii < np; ii++) {
            found.set(0);
            int max = 0;
            timer.tic();
            if (check)
                for (int jj = 0; jj < nb; jj++) {
                    q.offer( new Object() );
                    if (cnt++ > mod) {
                        int fnd = found.addAndGet( cnt );
                        if (fnd > max) max = fnd;
                        cnt = 0;
                    }
                }
            else
                for (int jj = 0; jj < nb; jj++)
                    q.offer( new Object() );
            double t1 = timer.tock();
            while (! q.isEmpty()) Simple.sleep(0);
            int fnd = found.addAndGet( cnt );
            double time = timer.tock();
            System.out.format( "time: %8.3f, t1:%8.3f, max:%5d, found:%5d\n", time, t1, max, fnd );
        }
        q.offer(msg);
    }

    // at 1.8 ghz

    // can que           4M objects in 0.75s
    // can que and count 4M objects in 0.78s (atomic, single access, blocks of 20)
    // can que and count 4M objects in 1.0 s (atomic, single access)
    //   seems very sensitive to jit ... as low as 1.0s, as high as 1.4s (no conditionals)


    // can que           1M objects in 0.20 s
    // can que and count 1M objects in 0.32 s (atomic)
    // seems to scale ok till at least .4M
    // access to local vars doesn't slow things down, but any access to a class member does
    //   ie, whether or not it's atomic

    
    
    public static void main(String [] args) {
        Simple.Scripts.cpufreq( "userspace", 1800000 );
        QueTest qt = new QueTest();
        qt.new Reader().start();
        qt.write();
        Simple.Scripts.cpufreq( "ondemand", 0 );
    }
    
}
