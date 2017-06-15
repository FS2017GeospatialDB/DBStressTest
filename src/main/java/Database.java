import com.datastax.driver.core.*;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("WeakerAccess")
public class Database {
    private static Cluster _cluster;
    private static Session _session;
    private static Map<String, PreparedStatement> _cache = new HashMap<>();

    /**
     * Disable Direct Instantiation
     */
    private Database() {}

    /**
     * Initialize the DB connection.
     */
    public static void initialize(List<InetAddress> contactPts) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL, 2, 8);
        _cluster = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoints(contactPts)
                .build();
        _session = _cluster.connect();
    }

    /**
     * Get a reference to the reused session object.
     */
    public static Session getSession() {
        return _session;
    }

    /**
     * Get a reference to the static cluster object.
     */
    public static Cluster getCluster() {return _cluster;}

    /**
     * Prepare a statement (or retrieve it from the cache if cached).
     */
    public static PreparedStatement prepareFromCache(String statement) {
        if (_cache.containsKey(statement))
            return _cache.get(statement);
        _cache.put(statement, _session.prepare(statement));
        return _cache.get(statement);
    }

    /**
     * Auto close the DB connection when the program quits
     */
    @Override
    protected void finalize() {
        try {
            super.finalize();
            if (_cluster != null)
                _cluster.close();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
