import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("WeakerAccess")
class fake_client implements Runnable {
    private static final int MAX_LNG = 360;
    private static final int MAX_LAT = 180;
    public static AtomicInteger query_per_second = new AtomicInteger(0);
    private static Random r = new Random();
    private static Session session = Database.getSession();
    private static PreparedStatement ps = Database.prepareFromCache("SELECT * FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");
    private Thread t;
    private String threadName;

    fake_client(String name) {
        threadName = name;
    }

    /**
     * This is a non-uniform level generator.
     * Given the desired distribution pattern, given back the level
     * THIS IS SO UGLY RIGHT?
     *
     * @return random level
     */
    private int genRandomLevel() {
        int ran = r.nextInt(100);
        if (ran < 30)
            return 13;
        else if (ran < 50)
            return 12;
        else if (ran < 60)
            return 11;
        else if (ran < 68)
            return 10;
        else if (ran < 75)
            return 9;
        else if (ran < 81)
            return 8;
        else if (ran < 86)
            return 7;
        else if (ran < 90)
            return 6;
        else if (ran < 94)
            return 5;
        else
            return 4;
    }

    private S2LatLng genRandomLatLng() {
        double lng = MAX_LNG * (0.5 - r.nextDouble());
        double lat = MAX_LAT * (0.5 - r.nextDouble());
        return S2LatLng.fromDegrees(lat, lng);
    }

    private S2CellId genRandomCell() {
        return S2CellId.fromLatLng(genRandomLatLng()).parent(genRandomLevel());
    }

    public void run() {
        while (true) {
            S2CellId cell = genRandomCell();
            ResultSet rs = session.execute(ps.bind(cell.level(), cell.id(), UUIDs.startOf(System.currentTimeMillis())));
            query_per_second.incrementAndGet();
        }
    }

    public void start() {
        if (t == null) {
            System.out.println("Starting " + threadName);
            t = new Thread(this, threadName);
            t.start();
        }
    }
}


public class testProcess {
    // TODO: initialize db later.
    private static Cluster cluster;
    private static Session session;

    // config things
    private static int NUM_THREAD = Runtime.getRuntime().availableProcessors();
    private static double[] BBOX = new double[4];
    private static List<InetAddress> CONTACTPTS;


    private static void setNumThread(String useNumCores, String numThread) {
        if (useNumCores.equalsIgnoreCase("false")) {
            NUM_THREAD = Integer.parseInt(numThread);
        }
    }

    private static void setContactPoints(String contactPoints) {
        String[] contactpts = contactPoints.split(",");
        System.out.print(contactpts);
        CONTACTPTS = new ArrayList<>();
        for (String host : contactpts) {
            try {
                CONTACTPTS.add(Inet4Address.getByName(host));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    private static void setBoundaryBox(String boundaryBox) {
        String[] bbox = boundaryBox.split(",");
        for (int i = 0; i < 4; i++) {
            BBOX[i] = Double.parseDouble(bbox[i]);
        }
    }

    private static void cfgLoader(String filename) {
        File configFile = new File(filename);

        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);

            String bbox = props.getProperty("bbox");
            String contactPts = props.getProperty("contact_points");
            String useNumThread = props.getProperty("use_num_thread");
            String useNumCores = props.getProperty("use_num_cores");

            setNumThread(useNumCores, useNumThread);
            setContactPoints(contactPts);
            setBoundaryBox(bbox);

            reader.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // post behavior
        Database.initialize(CONTACTPTS);
        cluster = Database.getCluster();
        session = Database.getSession();
    }

    public static void main(String args[]) throws InterruptedException {
        if (args.length != 1) {
            System.err.println("Usage: program config.properties");
            System.exit(1);
        }

        System.out.println(args[0]);
        cfgLoader(args[0]);


        fake_client[] tasks = new fake_client[NUM_THREAD];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new fake_client("Thread-" + i);
            tasks[i].start();
        }

        final LoadBalancingPolicy loadBalancingPolicy =
                cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
        final PoolingOptions poolingOptions =
                cluster.getConfiguration().getPoolingOptions();
        ScheduledExecutorService cluster_stat = Executors.newSingleThreadScheduledExecutor();
        cluster_stat.scheduleAtFixedRate(() -> {
            Session.State state = session.getState();
            for (Host host : state.getConnectedHosts()) {
                HostDistance distance = loadBalancingPolicy.distance(host);
                int connections = state.getOpenConnections(host);
                int inFlightQueries = state.getInFlightQueries(host);
                System.out.printf("%s connections=%d, current load=%d, max load = %d %n",
                        host, connections, inFlightQueries,
                        connections * poolingOptions.getMaxRequestsPerConnection(distance));
            }
        }, 5, 5, TimeUnit.SECONDS);

        ScheduledExecutorService qps_display = Executors.newSingleThreadScheduledExecutor();
        qps_display.scheduleAtFixedRate(() -> {
            System.out.println("QPS:" + fake_client.query_per_second.getAndSet(0));
        }, 1, 1, TimeUnit.SECONDS);
    }
}