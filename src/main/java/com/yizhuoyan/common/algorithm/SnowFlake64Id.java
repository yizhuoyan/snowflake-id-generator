package cn.itsource.bigmall.common.algorithm;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * Created by yizhuoyan on 2017/11/27 0027.
 * 基于SnowFlake的序列号生成实现, 64位ID (0+41(毫秒)+5(机器组ID)+5(机器ID)+12(重复累加))
 * <p>
 * （1）每个业务线、每个机房、每个机器生成的ID都是不同的
 * （2）同一个机器，每个毫秒内生成的ID都是不同的
 * （3）同一个机器，同一个毫秒内，以序列号区区分保证生成的ID是不同的
 * （4）将毫秒数放在最高位，保证生成的ID是趋势递增的
 */
public class SnowFlake64Id {
    private static final Logger log = Logger.getLogger(SnowFlake64Id.class.getName());
    //时间戳占2进制位数，默认41位
    private static final int TIMESTAMP_BITS = 41;
    //机器id所占2进制位数，默认5位
    private static final int WORKER_GROUP_ID_BITS = 5;
    //机器id所占2进制位数，默认5位
    private static final int WORKER_ID_BITS = 5;
    //生成序号占2进制位数，默认12位
    private static final int SEQUENCE_BITS = 12;

    //机器组id须偏移开始位置=12+5
    private static final int WORKER_GROUP_ID_OFFSET = SEQUENCE_BITS + WORKER_ID_BITS;
    //机器id须偏移开始位置=12
    private static final int WORKER_ID_OFFSET = SEQUENCE_BITS;
    //时间戳须偏移开始位置=12+5+5
    private static final int TIMESTAMP_OFFSET = SEQUENCE_BITS + WORKER_ID_BITS + WORKER_GROUP_ID_BITS;

    // 机器最大id=32
    private static final int MAX_WORKER_ID = ~(-1 << WORKER_ID_BITS);
    //机器所属组最大id=32
    private static final int MAX_WORKER_GROUP_ID = ~(-1 << WORKER_GROUP_ID_BITS);
    //最大序号=4096(表示1ms最大并发量)
    private static final int MAX_SEQUENCE = ~(-1 << SEQUENCE_BITS);

    //时间戳开始时间点，保证当前时间戳减去开始时间点的结果不超过41位2进制的最大值
    //private final static long TIMESTAMP_START_EPOCH = 1288834974657L;
    /**
     * 可通过系统属性设置 SnowFlake64Id.TIMESTAMP_START_EPOCH=2020-01-01T00:00:00Z
     * 默认从2020年1月1日开始
     */
    private static final long TIMESTAMP_START_EPOCH = Instant.parse(
            System.getProperty(
                    SnowFlake64Id.class.getSimpleName() + ".TIMESTAMP_START_EPOCH",
                    "2020-01-01T00:00:00Z")).toEpochMilli();

    /**
     * 当前机器所属组id+workId
     */
    private final int WORKERGROUP_AND_WORKER_ID;
    // 当前序号
    private volatile int sequence = 0;
    /**
     * 上一次生成后的时间戳
     */
    private volatile long lastTimestamp = 0;


    /**
     * @param workerId      机器id
     * @param workerGroupId 机房id
     * @throws IllegalArgumentException 如果workerId大于
     */
    public SnowFlake64Id(int workerId, int workerGroupId) {
        validateArgs(workerId, workerGroupId);
        this.WORKERGROUP_AND_WORKER_ID = (workerGroupId << WORKER_GROUP_ID_OFFSET) |
                (workerId << WORKER_ID_OFFSET);
    }

    private void validateArgs(int workerId, int workerGroupId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                    String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        if (workerGroupId > MAX_WORKER_GROUP_ID || workerGroupId < 0) {
            throw new IllegalArgumentException(
                    String.format("workerGroup Id can't be greater than %d or less than 0", MAX_WORKER_GROUP_ID));
        }

    }

    public SnowFlake64Id(int workerId) {
        int workerGroupId = Integer.getInteger(this.getClass().getSimpleName() + ".WORKER_ID", 0);
        validateArgs(workerId, workerGroupId);
        this.WORKERGROUP_AND_WORKER_ID = (workerGroupId << WORKER_GROUP_ID_OFFSET) |
                (workerId << WORKER_ID_OFFSET);
    }

    public SnowFlake64Id() {
        int workerGroupId = Integer.getInteger(this.getClass().getSimpleName() + ".WORKER_ID", 0);
        int workerId = Integer.getInteger(this.getClass().getSimpleName() + ".WORKER_ID",
                tryLocalMACForWorkerId());
        validateArgs(workerId, workerGroupId);
        this.WORKERGROUP_AND_WORKER_ID = (workerGroupId << WORKER_GROUP_ID_OFFSET) |
                (workerId << WORKER_ID_OFFSET);
    }

    /**
     * 尝试获取当前机器MAC地址作为workId
     *
     * @return worker-id must range from 0 to 1023
     */
    private static int tryLocalMACForWorkerId() {
        try {
            //获取本机mac地址
            InetAddress localHost = InetAddress.getLocalHost();
            NetworkInterface inetAddress = NetworkInterface.getByInetAddress(localHost);
            byte[] mac = inetAddress.getHardwareAddress();
            int total = 0;
            for (int i = mac.length; i-- > 0; ) {
                total += (mac[i] & 0xff);
            }
            log.info("use local mac address for worker-id");
            return Math.abs(total) % 1023;
        } catch (Exception e) {
            log.warning("try local mac failed，use random worker-id!!! no suit for product model.");
            return ThreadLocalRandom.current().nextInt() % 1023;
        }
    }

    /**
     * 生成下一个可用id
     *
     * @return 可用id
     */
    public synchronized long nextId() {
        // 当前时间戳
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            //Clock moved backwards. Refusing to generate
            throw new RuntimeException("should never happen");
        }
        //同一个毫秒内，增加序列号
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            //毫秒数用完，使用下一个时间戳
            if (sequence == 0) {
                timestamp = utilNextMillis(lastTimestamp);
                //保存本次使用时间戳
                lastTimestamp = timestamp;
            }
        } else {
            //不同毫秒，满足“取模随机性”的需求，重置时随机生成
            sequence = ThreadLocalRandom.current().nextInt(1 << 5);
        }
        return ((timestamp - TIMESTAMP_START_EPOCH) << TIMESTAMP_OFFSET) | WORKERGROUP_AND_WORKER_ID
                | sequence;
    }

    /**
     * 自旋，保证毫秒数大于传入毫秒数
     *
     * @param lastTimestamp 传入毫秒数
     * @return 最近毫秒数
     */
    private long utilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    //---------------测试---------------
    public static void main(String[] args) {
        SnowFlake64Id worker = new SnowFlake64Id(1);
        for (int i = 0; i < 30; i++) {
            System.out.println(worker.nextId());
        }
    }
}
