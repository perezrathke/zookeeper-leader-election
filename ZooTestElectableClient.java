/**
 * ZooTestElectableClient
 *
 */

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

// See http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
public class ZooTestElectableClient extends ZooElectableClient {
	
	boolean isFirstRun = true;
	boolean wasLeader = false;
	
	public ZooTestElectableClient() throws KeeperException, IOException, InterruptedException {
		super();
	}
	
	// Override this function to determine what work should be done
	void performRole() {
		
		if ( isFirstRun || ( wasLeader != getCachedIsLeader() ) ) {
			System.out.println( "ZooTestElectableClient::performRole:: work performed (" + getElectionGUIDZNodePath() + ") with state (leader=" + getCachedIsLeader() + ", isFirstRun=" + isFirstRun + ", wasLeader=" + wasLeader + ")" );			
		}
		else {
			System.out.println( "ZooTestElectableClient::performRole:: work  was not performed (" + getElectionGUIDZNodePath() + ")" );
		}
		
		isFirstRun = false;
		wasLeader = getCachedIsLeader();
	}
	
	// Main entry point
    public static void main(String args[])
        throws KeeperException, IOException, InterruptedException {
        ZooElectableClient zooClient = new ZooTestElectableClient();
        zooClient.run();
        System.out.println( "ZooTestElectableClient::main:: client finished." );
    }
}