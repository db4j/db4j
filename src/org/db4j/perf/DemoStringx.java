// copyright 2017 nqzero - see License.txt for terms

package org.db4j.perf;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.HunkCount;
import org.srlutils.Simple;

public class DemoStringx {
    public static class Chat1 extends Database {
        HunkCount count;
        Btrees.IS users;
        static Random src = new Random();

        public void route(Db4j.Connection conn) {
            String bio = RandomStringUtils.randomAlphanumeric(src.nextInt(8000));
            db4j.submitCall(txn -> {
                int num = count.get(txn);
                users.insert(txn,num,bio);
                count.plus(txn,1);
            });
        }
        String saved = null;
        
        public String info() {
            String val =
            db4j.submit(txn -> {
                String last = null;
                String [] all = users.getall(txn).vals().toArray(new String[0]);
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
        String filename = DemoHunker.resolve("./db_files/hunk2.mmap");

        if (args.length==0) {
            Db4j db4j = hello.start(filename,true);
            Db4j.Connection conn = db4j.connect();
            for (int ii = 0; ii < 3000; ii++)
                hello.route(conn);
            conn.awaitb();
            hello.info();
            hello.shutdown(true);
        }
        else 
        {
            hello.start(filename,false);
            hello.info();
        }
        System.exit(0);
    }
    
}
