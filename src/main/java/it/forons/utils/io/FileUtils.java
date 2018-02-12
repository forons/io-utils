package it.forons.utils.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

  private static Logger log = LoggerFactory.getLogger(FileUtils.class.getName());

  private static Configuration conf;

  /**
   * Used only for Java Beans
   */
  public FileUtils() {
    super();
  }

  private static Configuration initConfiguration() {
    if (conf == null) {
      conf = new Configuration();
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }
    return conf;
  }

  public static String readFile(String filepath) throws IOException {
    return readFile(filepath, initConfiguration());
  }

  public static String readFile(String filepath, Configuration conf) throws IOException {
    Path path = new Path(filepath);
    FileSystem fileSystem = FileSystem.get(path.toUri(), conf);

    if (!fileSystem.exists(path)) {
      log.debug("File " + filepath + " does not exists");
      return null;
    }

    try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)))) {
      return br.lines().collect(Collectors.joining("\n"));
    } finally {
      fileSystem.close();
    }
  }


  public static boolean writeFile(String content, String filepath) throws IOException {
    return writeFile(content, filepath, initConfiguration());
  }

  public static boolean writeFile(String content, String filepath, Configuration conf)
      throws IOException {
    return writeFile(content, filepath, true, conf);
  }

  public static boolean writeFile(String content, String filepath, boolean overwrite)
      throws IOException {
    return writeFile(content, filepath, true, initConfiguration());
  }

  public static boolean writeFile(String content, String filepath, boolean overwrite,
      Configuration conf)
      throws IOException {
    Path path = new Path(filepath);
    FileSystem fileSystem = FileSystem.get(path.toUri(), conf);

    if (fileSystem.exists(path) && !overwrite) {
      return false;
    }

    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)))) {
      bw.write(content);
    } finally {
      fileSystem.close();
    }
    return true;
  }

  public static boolean deleteFile(String filepath) throws IOException {
    return deleteFile(filepath, true, initConfiguration());
  }

  public static boolean deleteFile(String filepath, boolean recursive) throws IOException {
    return deleteFile(filepath, true, initConfiguration());
  }

  public static boolean deleteFile(String filepath, boolean recursive, Configuration conf)
      throws IOException {
    Path path = new Path(filepath);
    FileSystem fileSystem = FileSystem.get(path.toUri(), conf);

    if (!fileSystem.exists(path)) {
      log.debug("File " + path.toString() + " does not exists");
      return false;
    }

    fileSystem.delete(path, recursive);
    fileSystem.close();
    return true;
  }

  public boolean exists(String filepath) throws IOException {
    return exists(filepath, initConfiguration());
  }

  public boolean exists(String filepath, Configuration conf) throws IOException {
    Path path = new Path(filepath);
    FileSystem fileSystem = FileSystem.get(path.toUri(), conf);

    return fileSystem.exists(path);
  }

  public void mkdir(String dir) throws IOException {
    mkdir(dir, initConfiguration());
  }

  public void mkdir(String dir, Configuration conf) throws IOException {
    Path path = new Path(dir);
    FileSystem fileSystem = FileSystem.get(path.toUri(), conf);

    if (fileSystem.exists(path)) {
      log.debug("Dir " + dir + " already not exists");
      return;
    }

    fileSystem.mkdirs(path);
    fileSystem.close();
  }
}
