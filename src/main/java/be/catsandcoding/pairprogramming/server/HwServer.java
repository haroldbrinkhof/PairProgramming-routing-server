package be.catsandcoding.pairprogramming.server;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.zeromq.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HwServer {

    public static void main(String[] args) throws Exception {
        Thread mainAction = new Thread(new ServerTask());
        mainAction.start();
        while(mainAction.isAlive()){
            Thread.sleep(200);
        }

    }

    private static class ServerTask implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                ZMQ.Socket frontend = ctx.createSocket(SocketType.ROUTER);
                frontend.bind("tcp://*:5570");

                ZMQ.Socket backend = ctx.createSocket(SocketType.DEALER);
                backend.bind("inproc://backend");

                ZMQ.Socket publish = ctx.createSocket((SocketType.PUB));
                publish.bind("tcp://*:5571");

                new Thread(new PublishAll(ctx, publish)).start();

                ZMQ.proxy(frontend, backend, null);
            }
        }
    }

    private static class PublishAll implements Runnable {
        private final ZContext ctx;
        private final ZMQ.Socket pub;

        public PublishAll(ZContext ctx,ZMQ.Socket pub) {
            this.ctx = ctx;
            this.pub = pub;
        }

        @Override
        public void run() {
            ZMQ.Socket worker = ctx.createSocket(SocketType.DEALER);
            worker.connect("inproc://backend");

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg msg = ZMsg.recvMsg(worker);
                ZFrame address = msg.pop();
                ZFrame content = msg.pop();
                System.out.println(String.format("received {%s} {%s}", address, content));
                assert (content != null);
                msg.destroy();

                System.out.println("sending: " + content.toString());
                try {
                    TopicAssigner topicAssigner = new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                            .readValue(content.toString(), TopicAssigner.class);
                    pub.sendMore(topicAssigner.getSessionId());
                    pub.send(content.toString());
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                } finally {
                    address.destroy();
                    content.destroy();
                }
            }
            ctx.destroy();
        }
    }
}

