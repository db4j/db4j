// copyright 2017 nqzero - see License.txt for terms

package demo;

import org.eclipse.jetty.server.Server;
import tutorial.Chat;
import static org.db4j.perf.DemoHunker.resolve;

public class DemoJetty {
    
    public static void main(String[] args) throws Exception {
        Chat chat = new Chat();
        chat.start(resolve("./db_files/chat.mmap"),args.length > 0);
        Server server = new Server(8080);
        server.setHandler(new kilim.support.JettyHandler( (target,raw,req,resp) -> chat.route(req.getPathInfo()) ));
        server.start();
    }
    
}
