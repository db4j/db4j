package org.db4j.perf;


import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.Db4j;
import org.db4j.HunkCount;

// copyright 2017 nqzero - see License.txt for terms



public class DemoToast extends Database {
    HunkCount count;
    Btrees.IX users;
    static int usize = 1440;
    
    void addUser(Db4j.Connection conn) {
        byte [] user = new byte[usize];
        conn.submitCall(tid -> {
            int index = count.plus(tid,1);
            users.insert(tid,index,user);
            if (index%10 == 0) System.out.println("user: " + index);
        });
    }
    void getUser(Db4j.Connection conn) {
        conn.submitCall(tid -> {
            int index = 9;
            byte[] find = users.find(tid,index);
            System.out.println("user: " + find.length);
        });
    }
    
    public static void main(String[] args) {
        DemoToast hello = new DemoToast();
        String filename = DemoHunker.resolve("./db_files/hunk2.mmap");

        if (args.length==0) {
            int num = usize > 1000 ? 200 : 2000;
            Db4j db4j = hello.start(filename,true);
            Db4j.Connection conn = db4j.connect();
            for (int ii=0; ii < num; ii++)
                hello.addUser(conn);
            conn.awaitb();
            hello.shutdown(true);
        }
//        else 
        {
            Db4j db4j = hello.start(filename,false);
            Db4j.Connection conn = db4j.connect();
            hello.getUser(conn);
            conn.awaitb();
        }
        System.exit(0);
    }
    
}
