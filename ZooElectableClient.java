/**
 * ZooElectableClient
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
public abstract class ZooElectableClient implements Watcher, ZooZNodeDeletionMonitor.ZooZNodeDeletionMonitorListener {
    
	// The path to the election znode
	private static final String DEFAULT_ELECTION_ZNODE_PATH = "/election";
		
    // A string of server hosts
    private static final String DEFAULT_SERVER_HOSTS = "localhost:2181";

	// The time out interval in milliseconds
	private static final int DEFAULT_TIME_OUT_MS = 3000;
	
	// The ZooKeeper API handle
    private ZooKeeper hZooKeeper = null;

	// True if we are the leader, False otherwise
	private boolean isLeader = false;
	
	// Monitors existence of a parameter zNode
	ZooZNodeDeletionMonitor zNodeDeletionMonitor = null;
	
	// The path to our election GUID znode
	private String electionGUIDZNodePath = null;
	
	// @return String containing path to persistent election znode
	private static final String getElectionZNodePath() { return DEFAULT_ELECTION_ZNODE_PATH; }
	
	// @return String containing "host:port" pairs
	private static final String getHosts() { return DEFAULT_SERVER_HOSTS; }

	// @return integer containing time out interval in milliseconds
	private static int getTimeOutMs() { return DEFAULT_TIME_OUT_MS; }
	
	// @return handle to ZooKeeper API
	private ZooKeeper getZooKeeper() {
		assert null != hZooKeeper;
		return hZooKeeper;
	}
	
	// @return True if the we are the current leader, False otherwise
	protected boolean getCachedIsLeader() { return isLeader; }
	
	// @return handle to our deletion monitor
	private ZooZNodeDeletionMonitor getZNodeDeletionMonitor() {
		assert null != zNodeDeletionMonitor;
		return zNodeDeletionMonitor;
	}
	
	// @return the path of this client's election GUID
	protected final String getElectionGUIDZNodePath() {
		assert null != electionGUIDZNodePath;
		return electionGUIDZNodePath;
	}
	
	// Utility function to convert a GUID to a full path
	private String formatElectionGUIDZNodePath( String zNodeGUID ) {
		return getElectionZNodePath() + "/" + zNodeGUID;
	}
		
	// @return the path of the leader's election GUID
	private String getLeaderElectionGUIDZNodePath( List<String> optionalGUIDs ) throws KeeperException, InterruptedException {
		List<String> guids = ((optionalGUIDs == null) || optionalGUIDs.isEmpty()) ?
			getZooKeeper().getChildren( getElectionZNodePath(), false /*bWatch*/ ) : optionalGUIDs;
		if ( !guids.isEmpty() ) {
			String leaderGUID = formatElectionGUIDZNodePath( Collections.min( guids ) );
			System.out.println( "ZooElectableClient::getLeaderElectionGUIDZNodePath:: " + leaderGUID );
			return leaderGUID;
		}
		else {
			System.out.println( "ZooElectableClient::getLeaderElectionGUIDZNodePath:: no GUIDS exist!" );
			return null;
		}
	}
	
	// @return largest guid znode that is less than our guid (unless we are the leader, then return leader znode path)
	private String getZNodePathToWatch( List<String> optionalGUIDs ) throws KeeperException, InterruptedException {
		// Early out if we are the leader
		if ( getCachedIsLeader() ) {
			System.out.println( "ZooElectableClient::getZNodePathToWatch:: (" + getElectionGUIDZNodePath() + ") -> " + getElectionGUIDZNodePath() );
			return getElectionGUIDZNodePath();
		}
		
		List<String> guids = ((optionalGUIDs == null) || optionalGUIDs.isEmpty()) ?
			getZooKeeper().getChildren( getElectionZNodePath(), false /*bWatch*/ ) : optionalGUIDs;
		
		if ( !guids.isEmpty() ) {
			// Initialize to first path less than our znode
			String zNodePathToWatch = null;
			int itrGUID = 0;
			for ( ; itrGUID < guids.size(); ++itrGUID ) {
				String guid = formatElectionGUIDZNodePath( guids.get( itrGUID ) );
				if ( guid.compareTo( getElectionGUIDZNodePath() ) < 0 ) {
					zNodePathToWatch = guid;
					break;
				}
			}
			
			// There should be at least one znode less than us
			assert null != zNodePathToWatch;
			
			// Find largest znode that's less than our znode
			for ( ; itrGUID < guids.size(); ++itrGUID ) {
				String guid = formatElectionGUIDZNodePath( guids.get( itrGUID ) );
				if ( 
				       ( guid.compareTo( zNodePathToWatch ) > 0 )
					&& ( guid.compareTo( getElectionGUIDZNodePath() ) < 0 )
				) {
					zNodePathToWatch = guid;
				}
			}
			System.out.println( "ZooElectableClient::getZNodePathToWatch:: (" + getElectionGUIDZNodePath() + ") -> " + zNodePathToWatch );
			return zNodePathToWatch;
		}
		else {
			System.out.println( "ZooElectableClient::getZNodePathToWatch:: no GUIDS exist!");
			return null;
		}
	}
	
	// Constructor
	protected ZooElectableClient() throws KeeperException, IOException, InterruptedException {
		// Initialize the ZooKeeper api
		hZooKeeper = new ZooKeeper( getHosts(), getTimeOutMs(), this );
		// Attempt to create the election znode parent
		conditionalCreateElectionZNode();
		// Create our election GUID
		createElectionGUIDZNode();
	}
		
	// Attempts to create an election znode if it doesn't already exist
	private void conditionalCreateElectionZNode() throws KeeperException, InterruptedException {
		if ( null == getZooKeeper().exists( getElectionZNodePath(), false /*bWatch*/ ) ) {
			try {
				final String path = getZooKeeper().create( getElectionZNodePath(), null /*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
				System.out.println( "ZooElectableClient::conditionalCreateElectionZNode:: created with path:" + path );
			}
			catch( KeeperException.NodeExistsException ne ) {
				System.out.println( "ZooElectableClient::conditionalCreateElectionZNode:: failed (NodeExistsException)" );
			}
		}
		else {
			System.out.println( "ZooElectableClient::conditionalCreateElectionZNode:: already created." );
		}
	}
	
	// Creates a sequential znode with a lifetime of these client process
	private void createElectionGUIDZNode() throws KeeperException, InterruptedException {
		// Create an empheral|sequential file
		electionGUIDZNodePath = getZooKeeper().create( getElectionZNodePath() + "/guid-", null /*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
		System.out.println( "ZooElectableClient::createElectionGUIDZNode:: created with path:" +  electionGUIDZNodePath );
	}

	// Elects a leader and caches the results for this client
	private void electAndCacheLeader() throws KeeperException, InterruptedException {
		isLeader = getElectionGUIDZNodePath().equals( getLeaderElectionGUIDZNodePath( null ) );
		System.out.println( "ZooElectableClient::electAndCacheLeader:: " + isLeader );
	}

	// Sets a deletion monitor on the next lowest election GUID (if we are the leader, then we listen to ourselves)
	private void resetZNodeDeletionMonitor() throws KeeperException, InterruptedException {
		// TODO: possible race condition:
		// If zNodeDeletionMonitor is not null and a watch event comes in, we may get some sort of null exception or
		// watch/processResult calls on a stale monitor.
		zNodeDeletionMonitor = new ZooZNodeDeletionMonitor( getZooKeeper(), getZNodePathToWatch( null ), this );
	}
	
	// Watcher callback
    public void process(WatchedEvent event) {
		// znode monitor can be null if we haven't initialized it yet
		if ( null != zNodeDeletionMonitor ) {
			// Forward to znode monitor
			getZNodeDeletionMonitor().process( event );
		}
    }
		
	// Callback when monitored znode is deleted
	public void onZNodeDeleted() {
		try {
			determineAndPerformRole();
		}
		catch (Exception e) {
			// do nothing!
		}
	}
	
	// Callback received when znode monitor dies
	public void onZooKeeperSessionClosed() {
		synchronized (this) {
			notifyAll();
		}
	}
	
	// Holds leader election, performs work based on results, and watches on a GUID
	private void determineAndPerformRole() throws KeeperException, InterruptedException {
		electAndCacheLeader();
		// Do work based on whether or not we are the leader
		performRole();
		// Set a deletion monitor if we are not the leader
		resetZNodeDeletionMonitor();
	}
		
	// Wait until monitor is dead
    public void run() throws KeeperException, IOException, InterruptedException {
		
		// Perform initial work based on whether we are the leader or not
		determineAndPerformRole();
		
		try {
			synchronized (this) {
				while (!getZNodeDeletionMonitor().getIsZooKeeperSessionClosed()) {
					wait();
				}
			}
		}
		catch (InterruptedException e) {}
    }

	// Override this function to determine what work should be done
	abstract void performRole();
}
