/**
 * A simple class that monitors the data and existence of a ZooKeeper
 * node. It uses asynchronous ZooKeeper APIs.
 */
import java.util.Arrays;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

// See http://zookeeper.apache.org/doc/current/javaExample.html#ch_Introduction
public class ZooZNodeDeletionMonitor implements Watcher, StatCallback {

	// The ZooKeeper API handle
    ZooKeeper hZooKeeper = null;

	// The ZNode that we're monitoring
    String zNodePath = null;

	// True if we're dead, False otherwise
    boolean isZooKeeperSessionClosed = false;

	// The listener object interested in changes to our monitored znode
    ZooZNodeDeletionMonitorListener listener = null;

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface ZooZNodeDeletionMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void onZNodeDeleted();

        /**
         * The ZooKeeper session is no longer valid.
         */
        void onZooKeeperSessionClosed();
    }

	/**
	 * @return handle to ZooKeeper API
	 */
	private ZooKeeper getZooKeeper() {
		assert null != hZooKeeper;
		return hZooKeeper;
	}
	
	/**
	 * @return Path to zNode that we're monitoring for existence
	 */
	private final String getZNodePath() {
		return zNodePath;
	}
	
	/**
	 * @return True if we're dead, False otherwise
	 */
	public boolean getIsZooKeeperSessionClosed() {
		return isZooKeeperSessionClosed;
	}
	
	/**
	 * @return Listener to callback when data changes
	 */
	private ZooZNodeDeletionMonitorListener getListener() {
		assert null != listener;
		return listener;
	}
	
	/**
	 * Constructor
	 */
    public ZooZNodeDeletionMonitor(ZooKeeper hZooKeeper, String zNodePath, ZooZNodeDeletionMonitorListener listener) {
        this.hZooKeeper = hZooKeeper;
        this.zNodePath = zNodePath;
        this.listener = listener;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        checkZNodeExistsAsync();
    }

	/**
	 * Utility function to close monitor and inform listener on session close
	 */
	private void onZooKeeperSessionClosed() {
		isZooKeeperSessionClosed = true;
		getListener().onZooKeeperSessionClosed();
	}

	/**
	 * Utility function to check on znode existence
	 */
	private void checkZNodeExistsAsync() {
		 getZooKeeper().exists( getZNodePath(), true /*bWatch*/, this /*AsyncCallback.StatCallback*/, null /*Object ctx*/) ;
	}

	/**
	 * Watcher callback
	 */
    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
				onZooKeeperSessionClosed();
                break;
            }
        } else {
            if (path != null && path.equals(getZNodePath())) {
                // Something has changed on the node, let's find out
                checkZNodeExistsAsync();
            }
        }
    }

	/**
	 * AsyncCallback.StatCallback
	 */
	@SuppressWarnings("deprecation")
    public void processResult(int rc, String path, Object ctx, Stat stat) {
		System.out.println( "ZooZNodeDeletionMonitor::processResult:: rc is " + rc );
       	if ( rc == Code.NoNode /*Code.NONODE.ordinal()*/ ) {
            getListener().onZNodeDeleted();
		}
		else if ( rc == Code.Ok /*Code.OK.ordinal()*/ ) {
			 // put logging or handle this case separately (zNode exists)
		}
        else if ( 
		           ( rc == Code.SessionExpired /*Code.SESSIONEXPIRED.ordinal()*/ )
			    || ( rc == Code.NoAuth /*Code.NOAUTH.ordinal()*/ )
		    ) {
			onZooKeeperSessionClosed();
		}
		else {
            // Retry errors
            checkZNodeExistsAsync();
        }		
    }
}