// copyright 2017 nqzero - licensed under the terms of the MIT license

import org.db4j.perf.TextSearchExample;

// just a placeholder for now - eventually the demos/examples/perf etc should get removed from the db4j jar
// at that time, TextSearchExample will probably move here
// this file is just here as a placeholder/convenience till then, since the Posts.txt data lives in this directory
public class Example {
    public static void main(String[] args) throws Exception {
        if (args.length==0)
            args = new String[]{"./doc/Posts.txt","../db_files/hunk2.mmap"};
        TextSearchExample.main(args);
    }

    
}
