// copyright 2017 nqzero - see License.txt for terms

package demo;

import kilim.demo.KilimHandler;
import org.eclipse.jetty.server.Server;
import tutorial.Chat;

public class DemoJetty {
    
    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();
        chat.start("../db_files/hunk2.mmap",args.length > 0);
        Server server = new Server(8080);
        server.setHandler(new KilimHandler( (target,raw,req,resp) -> chat.route(req.getPathInfo()) ));
        server.start();
    }
    
}
