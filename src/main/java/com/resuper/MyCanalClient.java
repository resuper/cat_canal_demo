package com.resuper;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class MyCanalClient {
    private static String SERVER_ADDRESS = "192.168.66.66";
    private static Integer PORT = 11111;
    private static String DESTINATION = "example";
    private static String USERNAME = "";
    private static String PASSWORD = "";

    public static void main(String[] args) throws InvalidProtocolBufferException {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(SERVER_ADDRESS, PORT),
                DESTINATION, USERNAME, PASSWORD);
        canalConnector.connect();
        canalConnector.subscribe(".*\\..*");
        canalConnector.rollback();

        while (true) {
            Message message = canalConnector.getWithoutAck(100);
            long messageId = message.getId();
            if (messageId != -1) {
                System.out.println("messageId===》" + messageId);
                printEntity(message.getEntries());
//                // 提交确认
//                canalConnector.ack(messageId);
//                // 处理失败，回滚数据
//                canalConnector.rollback(messageId);
            }
        }

    }

    private static void printEntity(List<CanalEntry.Entry> entries) throws InvalidProtocolBufferException {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            System.out.println("fdsfsdfds");
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                switch (rowChange.getEventType()) {
                    case INSERT: {
                        String tableName = entry.getHeader().getTableName();
                        System.out.println("表" + tableName + "做了insert操作:" + rowData.getAfterColumnsList());
                        break;
                    }
                    case DELETE: {
                        String tableName = entry.getHeader().getTableName();
                        System.out.println("表" + tableName + "做了delete操作:" + rowData.getAfterColumnsList());
                        break;
                    }
                    case UPDATE: {
                        String tableName = entry.getHeader().getTableName();
                        System.out.println("表" + tableName + "做了update操作:" + rowData.getAfterColumnsList());
                        break;
                    }
                    default: {
                        System.out.println("其他操作");
                        break;
                    }
                }

            }
        }
    }

}
