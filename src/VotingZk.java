import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("DuplicatedCode")
public class VotingZk implements Watcher {

    ZooKeeper zk;
    String zkAddress = "localhost:2181";
    String nodeID = Integer.toHexString(new Random().nextInt());
    List<String> votesNodes = new ArrayList<>();
    String followersVote;
    int yesVotes = 0;
    int noVotes = 0;

    private void register() throws InterruptedException {
        try {
            zk.create("/leader", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(nodeID + ": Leader created");
            waitVotes();
        } catch (KeeperException e) {
            System.out.println(nodeID + ": Becoming a follower, leader already exists.");
            follow();
        }
    }

    private void waitVotes() throws InterruptedException {
        try {
            votesNodes = zk.getChildren("/votes", this);
            if(votesNodes.size() == 3) {
                System.out.println("Voting done, collecting results..");
                collectVotes();
            }else{
                System.out.println(nodeID + ": waiting for voters to cast their vote...");
            }
        } catch (KeeperException e) {
            System.out.println(nodeID + ": Node /votes doesn't exist. Create it from command line please...");
            waitVotes();
        }
    }

    private void collectVotes() throws InterruptedException {
        try {
            for (String v : votesNodes) {
                byte[] vote = zk.getData("/votes/" + v, false, null);
                if (Arrays.equals(vote, "yes".getBytes())) {
                    yesVotes++;
                } else if (Arrays.equals(vote, "no".getBytes())) {
                    noVotes++;
                }
            }
            String result = yesVotes > noVotes ? "yes" : "no";
            Stat stat = zk.exists("/leader", false);
            System.out.println(nodeID + ": FINAL RESULT - " + result);
            zk.setData("/leader", result.getBytes(), stat.getVersion());
        } catch(KeeperException e) {
            e.printStackTrace();
        }
    }

    private void follow() throws InterruptedException {
        try {
           zk.getData("/leader", this, null);
           vote();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void vote() throws InterruptedException{
        try {
            followersVote = new Random().nextBoolean() ? "yes" : "no";
            System.out.println(nodeID + ": Voting - " + followersVote);
            zk.create("/votes/vote-", followersVote.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void getResults() throws InterruptedException {
        try {
            byte[] results = zk.getData("/leader", this, null);
            if(Arrays.equals(followersVote.getBytes(), results)){
                System.out.println(nodeID + ": Guessed correctly - VICTORY.");
            }else{
                System.out.println(nodeID + ": Guessed incorrectly - LOSS");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getPath().equals("/votes") && event.getType() == Event.EventType.NodeChildrenChanged){
            try {
                waitVotes();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(event.getPath().equals("/leader") && event.getType().equals(Event.EventType.NodeDataChanged)){
            try {
                getResults();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void start() throws IOException {
        zk = new ZooKeeper(zkAddress, 1000, this);
    }

    private void stop() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache.zookeeper.ClientCnxn").setLevel(Level.OFF);
        Logger.getLogger("org.apache.zookeeper.ZooKeeper").setLevel(Level.OFF);


        List<VotingZk> votingSimulationList = new ArrayList<>();

        for (int i = 0; i < 4; i++){
            votingSimulationList.add(new VotingZk());
        }

        for(VotingZk v : votingSimulationList) {
            v.start();
            v.register();
        }

        Thread.sleep(1000000000);

        for(VotingZk v : votingSimulationList) {
            v.stop();
        }
    }
}
