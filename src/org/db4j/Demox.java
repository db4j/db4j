// copyright 2017 nqzero - see License.txt for terms

package org.db4j;

import kilim.Pausable;

public class Demox extends Database {
    HunkCount count;
    Btrees.IX users;
    static int usize = 1440;
    
    void addUser() {
        byte [] user = new byte[usize];
        hunker.submitCall(tid -> {
            int index = count.plus(tid,1);
            users.insert(tid,index,user);
            if (index%10 == 0) System.out.println("user: " + index);
        });
    }
    void getUser() {
        hunker.submitCall(tid -> {
            int index = 9;
            byte[] find = users.find(tid,index);
            System.out.println("user: " + find.length);
        });
    }
    
    public static void main(String[] args) {
        Demox hello = new Demox();

        if (args.length==0) {
            int num = usize > 1000 ? 200 : 2000;
            hello.start("./db_files/hunk2.mmap",true);
            for (int ii=0; ii < num; ii++)
                hello.addUser();
            hello.hunker.fence(null,100);
            hello.shutdown(true);
        }
//        else 
        {
            hello.start("./db_files/hunk2.mmap",false);
            hello.getUser();
            hello.hunker.fence(null,100);
        }
        System.exit(0);
    }
    
}
