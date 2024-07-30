
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold = 50;
    private String walFilePath;

    //构造方法，初始化
    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndexes();
    }

    // 生成持久化文件的路径
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    // 重新加载索引数据
    public void reloadIndexes() {
        File dir = new File(this.dataDir); // 数据目录路径
        File[] files = dir.listFiles((dir1, name) -> name.matches("\\d{8}_\\d{6}\\.table"));
        List<String> fileNames = new ArrayList<>();
        // 将文件名存入列表
        if (files != null) {
            for (File file : files) {
                fileNames.add(file.getName());
            }
        }
        // 根据文件名排序文件列表
        Collections.sort(fileNames, new Comparator<String>() {
            @Override
            public int compare(String fileName1, String fileName2) {
                // 定义日期格式解析器
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");

                try {
                    // 解析文件名中的时间戳
                    Date date1 = dateFormat.parse(getTimestampFromFileName(fileName1));
                    Date date2 = dateFormat.parse(getTimestampFromFileName(fileName2));

                    // 比较日期
                    return date1.compareTo(date2);
                } catch (ParseException e) {
                    e.printStackTrace();
                    // Handle parsing exception
                    return 0; // or throw exception depending on your error handling strategy
                }
            }

            // 辅助方法：从文件名中提取时间戳
        private String getTimestampFromFileName(String fileName) {
            return fileName.substring(0, 15);
        }
    });

        // 按照排序后的文件名依次重新加载索引
    for (String fileName : fileNames) {
        reloadIndex(fileName);
    }
        // 重新加载主数据文件的索引
    reloadIndex("data.table");
    LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
}
private static final int MAX_COMMAND_LENGTH = 1024;
    public void reloadIndex(String fileName) {
        //定义字节数组用来读取文件中的数据
        byte[] bytes = new byte[MAX_COMMAND_LENGTH];
        //将传入的文件名与 dataDir 拼接，得到完整的文件路径。
        fileName = this.dataDir + File.separator + fileName;
        //循环读取指定长度的字节数据，转换为commend对象，将命令对象及其位置信息放入索引中
        try (RandomAccessFile file = new RandomAccessFile(fileName, RW_MODE)) {
            long len = file.length();
            long start = 0;
            while (start < len) {
                int cmdLen = file.readInt();
                if (cmdLen > bytes.length) {
                    bytes = new byte[cmdLen];
                }
                file.readFully(bytes, 0, cmdLen);
                JSONObject value = JSON.parseObject(new String(bytes, 0, cmdLen, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen , fileName);
                    index.put(command.getKey(),cmdPos);
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (IOException e) {
            LoggerUtil.error(LOGGER, null,"Failed to reload index");
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }

    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // 记录到WAL文件
            RandomAccessFileUtil.writeInt(this.genWalFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(this.genWalFilePath(), commandBytes);
            // 写入内存表
            memTable.put(key, command);

            // 检查内存表是否达到阈值
            if (memTable.size() >= storeThreshold) {
                persistMemTable(); // 持久化内存表
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void persistMemTable() throws IOException {
        // 将内存表中的数据写入磁盘
        for (Map.Entry<String, Command> entry : memTable.entrySet()) {
            byte[] commandBytes = JSONObject.toJSONBytes(entry.getValue());
            // 写table（wal）文件
            rotate();
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 更新索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length, this.genFilePath());
            index.put(entry.getKey(), cmdPos);
        }
        memTable.clear(); // 清空内存表
    }

    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 从内存表中查
            Command command = memTable.get(key);
            if (command instanceof SetCommand) {
                return ((SetCommand) command).getValue();
            }
            // 从索引中查找
            CommandPos cmdPos = index.get(key);
            System.out.println(cmdPos);
            if (cmdPos == null) {
                return null;
            }
            //从磁盘查
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(cmdPos.getFileName(), cmdPos.getPos(), cmdPos.getLen());
            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // 记录到WAL文件
            RandomAccessFileUtil.writeInt(this.genWalFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(this.genWalFilePath(), commandBytes);
            // 写入内存表
            memTable.put(key, command);

            // 检查内存表是否达到阈值
            if (memTable.size() >= storeThreshold) {
                persistMemTable(); // 持久化内存表
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    // 回放WAL文件
    private void replayWal() {
        try (RandomAccessFile walFile = new RandomAccessFile(walFilePath, "r")) {
            long len = walFile.length();
            long start = 0;
            byte[] bytes = new byte[1024];
            while (start < len) {
                int cmdLen = walFile.readInt();
                if (cmdLen > bytes.length) {
                    bytes = new byte[cmdLen];
                }
                walFile.readFully(bytes, 0, cmdLen);
                JSONObject value = JSONObject.parseObject(new String(bytes, 0, cmdLen, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                if (command instanceof SetCommand) {
                    memTable.put(command.getKey(), command);
                } else if (command instanceof RmCommand) {
                    memTable.put(command.getKey(), command);
                }
                start += 4 + cmdLen;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String genWalFilePath() {
        return this.walFilePath;
    }
    @Override
    public void close() throws IOException {

    }
    /*文件达到一定大小时：
    关闭当前文件，并检查其大小是否超过设定的最大限制。
    如果超出限制，则重命名当前文件，并将其压缩为 .zip 文件。
    清空索引，以便在新的文件中重新开始记录数据。*/
    private static long maxsizeKB = 50;
    public void rotate() throws IOException {
        RandomAccessFile file1 = new RandomAccessFile(this.genFilePath(), RW_MODE);
        long len = file1.length();
        file1.close();
        if (maxsizeKB<=len){
            File file = new File(this.genFilePath());
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String formattedDateTime = dateFormat.format(new Date());
            String newfilepath = this.dataDir + File.separator + formattedDateTime + TABLE;
            String zipfilepath = this.dataDir + File.separator + formattedDateTime + ".zip";
            File newfile = new File(newfilepath);
            boolean b = file.renameTo(newfile);
            zip(newfilepath,zipfilepath);
            index.clear();
        }
    }

    //多线程压缩
    //创建了一个固定大小为5的线程池，用于异步执行文件压缩任务
    private ExecutorService executor = Executors.newFixedThreadPool(5);
    public void zip(String sourceFile,String zipfile) {
        executor.submit(() ->{
            byte[] buffer = new byte[1024];
            try (FileOutputStream fos = new FileOutputStream(zipfile);
                 ZipOutputStream zos = new ZipOutputStream(fos);
                 FileInputStream fis = new FileInputStream(sourceFile)) {

                // 将文件添加到压缩流中
                ZipEntry ze = new ZipEntry(sourceFile);
                zos.putNextEntry(ze);

                int length;
                while ((length = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, length);
                }

                zos.closeEntry();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    //关闭线程池
    public void exe_shutdown(){
        executor.shutdown();
    }
}
