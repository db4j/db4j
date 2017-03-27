// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.HunkCount;
import org.srlutils.Simple;

public class DemoStringx {
    public static class Chat1 extends Database {
        HunkCount count;
        Btrees.IS users;
        static Random src = new Random();

        public void route() {
            String bio = RandomStringUtils.randomAlphanumeric(src.nextInt(8000));
            hunker.submitCall(tid -> {
                int num = count.get(tid);
                users.insert(tid,num,bio);
                count.plus(tid,1);
            });
        }
        String saved = null;
        
        public String info() {
            String val =
            hunker.submit(tid -> {
                String last = null;
                String [] all = users.getall(tid).vals().toArray(new String[0]);
                for (String user : all) {
                    last = user;
                    if (all.length < 20) System.out.println(last);
                }
                return last;
            }).awaitb().val;
            if (saved==null) saved = val;
            else Simple.hardAssert(saved.equals(val));
            System.out.println(val.substring(0,30));
            return null;
        }
    }

    public static void main(String[] args) {
        Chat1 hello = new Chat1();

        if (args.length==1) {
            hello.start("./db_files/hunk2.mmap",true);
            for (int ii = 0; ii < 3000; ii++)
                hello.route();
            hello.hunker.fence(null,100);
            hello.info();
            hello.shutdown(true);
        }
        else 
        {
            hello.start("./db_files/hunk2.mmap",false);
            hello.info();
        }
        System.exit(0);
    }
    
}
