// Max Blankestijn & Rintse van de Vlasakker
// A fixed version of the SocketSpout provided with storm.

// Corrects a race condition between the contructor and the activate() function.

// The most notable alterations are in the constructor and activate() function:
// Moved the creation and start of the reader thread to activate()

// Original SocketSpout:
// https://github.com/apache/storm/blob/master/sql/storm-sql-runtime/src/jvm/org/apache/storm/sql/runtime/datasource/socket/spout/SocketSpout.java

package aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.Config;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;


public class FixedSocketSpout implements IRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FixedSocketSpout.class);

    private final String host;
    private final int port;
    private final Scheme scheme;

    private volatile boolean running;

    private BlockingDeque<List<Object>> queue;
    private Socket socket;
    private Thread readerThread;
    private BufferedReader in;
    private ObjectMapper objectMapper;

    private SpoutOutputCollector collector;
    private Map<String, List<Object>> emitted;

    public FixedSocketSpout(Scheme scheme, String host, int port) {
        this.scheme = scheme;
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.queue = new LinkedBlockingDeque<>();
        this.emitted = new HashMap<>();
        this.objectMapper = new ObjectMapper();

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Socket connecting to " + host + ":" + port);
        } catch (IOException e) {
            throw new RuntimeException("Error opening socket: host " + host + " port " + port);
        }
    }

    @Override
    public void close() {
        running = false;
        readerThread.interrupt();
        queue.clear();
        closeQuietly(in);
        closeQuietly(socket);
    }

    @Override
    public void activate() {
        running = true;
        readerThread = new Thread(new SocketReaderRunnable());
        // Only start the readerthread when activated, so that running is true
        // when SocketReaderRunnable.run() is called
        readerThread.start();
    }

    @Override
    public void deactivate() {
        running = false;
    }

    @Override
    public void nextTuple() {
        if (queue.peek() != null) {
            List<Object> values = queue.poll();
            if (values != null) {
                String id = UUID.randomUUID().toString();
                emitted.put(id, values);
                collector.emit(values, id);
            }
        }
    }

    private List<Object> convertLineToTuple(String line) {
        return scheme.deserialize(ByteBuffer.wrap(line.getBytes()));
    }

    @Override
    public void ack(Object msgId) {
        emitted.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        List<Object> emittedValues = emitted.remove(msgId);
        if (emittedValues != null) {
            queue.addLast(emittedValues);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    private class SocketReaderRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    String line = in.readLine();
                    if (line == null) {
                        throw new RuntimeException("EOF reached from the socket.");
                    }
                    List<Object> values = convertLineToTuple(line.trim());
                    queue.push(values);
                } catch (Throwable t) {
                    die(t);
                }
            }
        }
    }

    private void die(Throwable t) {
        LOG.error("Halting process: FixedSocketSpout died.", t);
        if (running || (t instanceof Error)) { System.exit(11); }
    }

    private void closeQuietly(final Closeable closeable) {
        try { if (closeable != null) { closeable.close(); } }
        catch (final IOException ioe) { /* ignore */ }
    }
}
