package com.yizhuoyan.common.algorithm;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by yizhuoyan on 2017/11/27 0027.
 * 基于SnowFlake的序列号生成实现, 64位ID (42(毫秒)+5(机器ID)+5(业务编码)+12(重复累加))
 *
 * （1）每个业务线、每个机房、每个机器生成的ID都是不同的
 * （2）同一个机器，每个毫秒内生成的ID都是不同的
 * （3）同一个机器，同一个毫秒内，以序列号区区分保证生成的ID是不同的
 * （4）将毫秒数放在最高位，保证生成的ID是趋势递增的
 */
public class SnowFlakeUUID {

        //起始时间戳
        private final static long TWEPOCH = 1288834974657L;

        // 机器标识位数
        private final static int WORKER_ID_BITS =5;

        // 数据中心标识位数
        private final static int DATA_CENTER_ID_BITS = 5;

        //机器唯一id
        private final static int MACHINE_ID_BITS=WORKER_ID_BITS+DATA_CENTER_ID_BITS;

        // 数据中心ID最大值 31
        private final static int MAX_DATA_CENTER_ID = ~(-1 << DATA_CENTER_ID_BITS);

        // 机器ID最大值 31
        private final static int MAX_WORKER_ID = ~(-1 << WORKER_ID_BITS);

        private final static int MAX_MACHINE_ID =~(-1<<MACHINE_ID_BITS);

        // 毫秒内自增位
        private final static int SEQUENCE_BITS = 12;

        // 机器ID偏左移12位
        private final static int WORKER_ID_SHIFT = SEQUENCE_BITS;

        private final static int DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

        // 时间毫秒左移22位
        private final static int TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;

        private final static int SEQUENCE_MASK = ~ (-1<< SEQUENCE_BITS);

        private long lastTimestamp = -1L;

        private long sequence = 0L;
        private final int workerId;
        private final int dataCenterId;
        private final static  SnowFlakeUUID LOCAL_MACHINE_MAC_INSTANCE=new SnowFlakeUUID(getLocalMachineMACInt());

    public static long uuid(){
        return LOCAL_MACHINE_MAC_INSTANCE.nextValue();
    }

   //machineId must range from 0 to 1023
    private static int getLocalMachineMACInt(){
        try {
            //获取本机mac地址
            InetAddress localHost = InetAddress.getLocalHost();
            NetworkInterface inetAddress = NetworkInterface.getByInetAddress(localHost);
            byte[] mac = inetAddress.getHardwareAddress();
            int total = 0;
            for (int i = mac.length; i-- > 0; ) {
                total += (mac[i] & 0xff);
            }

            return Math.abs(total)%1023;
        }catch (Exception e){
            return ThreadLocalRandom.current().nextInt()%1023;
        }
    }
    public static void main(String[] args)throws Exception{
        System.out.println(getLocalMachineMACInt());
    }

    public  SnowFlakeUUID(int machineId) {
        if(machineId> MAX_MACHINE_ID ||machineId<0){
            throw  new IllegalArgumentException(String.format("machineId must range from %d to %d", 0,
                    MAX_MACHINE_ID));
        }
        this.workerId = machineId>>WORKER_ID_BITS;
        this.dataCenterId = machineId&MAX_DATA_CENTER_ID;
    }

    private SnowFlakeUUID(int workerId, int dataCenterId) {
            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(String.format("workerId must range from %d to %d", 0,
                        MAX_WORKER_ID));
            }

            if (dataCenterId > MAX_DATA_CENTER_ID || dataCenterId < 0) {
                throw new IllegalArgumentException(String.format("dataCenterId must range from %d to %d", 0,
                        MAX_DATA_CENTER_ID));
            }

            this.workerId = workerId;
            this.dataCenterId = dataCenterId;
        }

       private synchronized long nextValue() {
            long timestamp = time();
            if (timestamp < lastTimestamp) {
                throw new RuntimeException("Clock moved backwards, refuse to generate id for "
                        + (lastTimestamp - timestamp) + " milliseconds");
            }
            if (lastTimestamp == timestamp) {
                // 当前毫秒内，则+1
                sequence = (sequence + 1) & SEQUENCE_MASK;
                if (sequence == 0) {
                    // 当前毫秒内计数满了，则等待下一秒
                    timestamp = tilNextMillis(lastTimestamp);
                }
            } else {
                //满足“取模随机性”的需求，重置时随机生成
                sequence = ThreadLocalRandom.current().nextInt(10);
            }
            lastTimestamp = timestamp;

            // ID偏移组合生成最终的ID，并返回ID
            long nextId = ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT)
                    | (dataCenterId << DATA_CENTER_ID_SHIFT) | (workerId << WORKER_ID_SHIFT) | sequence;

            return nextId;
        }

        private final long tilNextMillis(final long lastTimestamp) {
            long timestamp = this.time();
            while (timestamp <= lastTimestamp) {
                timestamp = this.time();
            }
            return timestamp;
        }

        private final long time() {
            return System.currentTimeMillis();
        }

}
