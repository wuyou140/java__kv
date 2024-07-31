
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
    //持久化阈值
    private static final int MAX_COMMAND_LENGTH = 1024;
    private static long maxsizeKB = 50;
    private final int storeThreshold = 50;
    private final String walFilePath;

    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    //数据目录
    private final String dataDir;
    //读写锁
    private final ReadWriteLock indexLock = new ReentrantReadWriteLock();
    //内存表
    private final TreeMap<String, Command> memTable = new TreeMap<>();
    //hash索引，存的是数据长度和偏移量
    private final HashMap<String, CommandPos> index = new HashMap<>();
    //线程池
    private ExecutorService deduplicationExecutor = Executors.newFixedThreadPool(5);
    private String currentFilePath;


    // 构造方法，初始化
    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist, creating...");
            file.mkdirs();
        }
        this.walFilePath = this.genWalFilePath();
        this.reloadIndexes();
        this.replayWal();
        this.currentFilePath = this.genFilePath();
    }

    // 生成持久化文件的路径
    private String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }


    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;




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

//重新加载索引
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
                try {
                    persistMemTable();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
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
                // 确保文件中有足够的数据来读取 int 类型的 cmdLen
                /*if (len - start < 4) {
                    throw new IOException("File truncated or corrupted: not enough data for cmdLen.");
                }*/

                int cmdLen = walFile.readInt();
                // 调整 buffer 大小以适应 cmdLen
                if (cmdLen > bytes.length) {
                    bytes = new byte[cmdLen];
                }

                // 确保文件中有足够的数据来读取 cmdLen 长度的数据
                /*if (len - start < 4 + cmdLen) {
                    throw new IOException("File truncated or corrupted: not enough data for command.");
                }*/

                walFile.readFully(bytes, 0, cmdLen);
                JSONObject value = JSONObject.parseObject(new String(bytes, 0, cmdLen, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                if (command instanceof SetCommand || command instanceof RmCommand) {
                    memTable.put(command.getKey(), command);
                }

                start += 4 + cmdLen; // 4 bytes for int cmdLen
            }
        } catch (EOFException e) {
            System.err.println("EOFException encountered: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("IOException encountered: " + e.getMessage());
        }
    }
    private String genWalFilePath() {
        return "D:/ideaproject/nosqldatebase/logs/nosql.log";
    }



    public synchronized void rotate() throws IOException {
        RandomAccessFile currentFile = new RandomAccessFile(this.currentFilePath, "rw");
        long len = currentFile.length();
        currentFile.close();

        if (len >= maxsizeKB * 1024) {
            // 创建新文件名，例如：data_yyyyMMdd_HHmmss.table
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String formattedDateTime = dateFormat.format(new Date());
            String newFileName = this.dataDir + File.separator + NAME + "_" + formattedDateTime + TABLE;

            // 重命名当前文件
            File currentFileObject = new File(this.currentFilePath);
            File newFileObject = new File(newFileName);
            boolean renamed = currentFileObject.renameTo(newFileObject);

            if (renamed) {
                // 更新当前文件路径为新文件路径
                this.currentFilePath = newFileName;
                // 清空索引，准备在新文件中记录数据
                this.index.clear();
                // 可以添加日志记录操作
                LoggerUtil.info(LOGGER, logFormat, "rotate", "Rotated to new file: " + newFileName);
            } else {
                // 处理重命名失败的情况
                LoggerUtil.error(LOGGER, null, "Failed to rotate file");
            }
        }
    }

    private void persistMemTable() throws IOException {
        // 从内存表中获取所有命令并添加到列表中
        List<Command> commands = new ArrayList<>(memTable.values());

        // 如果内存表中的命令数量大于0，则继续处理
        if (!commands.isEmpty()) {
            // 调用 rotate 方法检查是否需要滚动 WAL 文件
            rotate();

            // 为去重操作创建一个线程池，如果已有线程池则使用现有线程池
            ExecutorService deduplicationExecutor = Executors.newFixedThreadPool(5);

            try {
                // 提交去重任务到线程池
                for (Command command : commands) {
                    deduplicationExecutor.submit(() -> {
                        try {
                            deduplicateAndWriteToDisk(command);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } finally {
                // 关闭线程池
                deduplicationExecutor.shutdown();
            }
        }

        // 清空内存表
        memTable.clear();
    }

    // 执行单个命令的数据去重和写入磁盘
    private void deduplicateAndWriteToDisk(Command command) throws IOException {
        // 检查索引，确定是否已存在相同的 key
        if (!index.containsKey(command.getKey())) {
            // 写入新数据到磁盘
            try (RandomAccessFile file = new RandomAccessFile(this.genFilePath(), "rw")) {
                byte[] commandBytes = JSONObject.toJSONBytes(command);
                file.writeInt(commandBytes.length);
                file.write(commandBytes);

                // 更新索引
                CommandPos cmdPos = new CommandPos((int) file.getFilePointer(), commandBytes.length, this.genFilePath());
                index.put(command.getKey(), cmdPos);
            }
        }
    }

    // 检查磁盘上是否已存在相同的key
    private boolean isKeyExistsOnDisk(String key) {
        // 检查索引，如果索引中存在key，则认为已存在
        return index.containsKey(key);
    }


    //关闭线程池
    @Override
    public void close() throws IOException {
        // 关闭去重线程池
        if (!deduplicationExecutor.isShutdown()) {
            deduplicationExecutor.shutdown();
        }
    }
}
