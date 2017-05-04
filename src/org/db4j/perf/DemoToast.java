package org.db4j.perf;


import org.db4j.Btrees;
import org.db4j.Database;
import org.db4j.HunkCount;

// copyright 2017 nqzero - see License.txt for terms



public class DemoToast extends Database {
    HunkCount count;
    Btrees.IX users;
    static int usize = 1440;
    
    void addUser() {
        byte [] user = new byte[usize];
        db4j.submitCall(tid -> {
            int index = count.plus(tid,1);
            users.insert(tid,index,user);
            if (index%10 == 0) System.out.println("user: " + index);
        });
    }
    void getUser() {
        db4j.submitCall(tid -> {
            int index = 9;
            byte[] find = users.find(tid,index);
            System.out.println("user: " + find.length);
        });
    }
    
    public static void main(String[] args) {
        DemoToast hello = new DemoToast();

        if (args.length==0) {
            int num = usize > 1000 ? 200 : 2000;
            hello.start("./db_files/hunk2.mmap",true);
            for (int ii=0; ii < num; ii++)
                hello.addUser();
            hello.db4j.fence(null,100);
            hello.shutdown(true);
        }
//        else 
        {
            hello.start("./db_files/hunk2.mmap",false);
            hello.getUser();
            hello.db4j.fence(null,100);
        }
        System.exit(0);
    }
    
}
