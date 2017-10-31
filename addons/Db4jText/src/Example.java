// copyright 2017 nqzero - licensed under the terms of the MIT license

import org.db4j.perf.TextSearchExample;

public class Example {
    public static void main(String[] args) throws Exception {
        if (args.length==0)
            args = new String[]{"./doc/Posts.txt","../../db_files/hunk2.mmap"};
        TextSearchExample.main(args);
    }

    
}
