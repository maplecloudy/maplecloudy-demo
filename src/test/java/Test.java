import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.collections.CollectionUtils;
import org.eclipse.jgit.api.ArchiveCommand;
import org.eclipse.jgit.api.ArchiveCommand.Format;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.spark_project.guava.collect.Lists;

public class Test {
  public static void main(String[] args) throws Exception {
//    long current = System.currentTimeMillis();// 当前时间毫秒数
//    long zero = current / (1000 * 3600 * 24) * (1000 * 3600 * 24)
//        - TimeZone.getDefault().getRawOffset();// 今天零点零分零秒的毫秒数
//    long twelve = zero + 24 * 60 * 60 * 1000 - 1;// 今天23点59分59秒的毫秒数
//    System.out.println(current);// 当前时间
//    System.out.println(zero);// 今天零点零分零秒
//    System.out.println(twelve);// 今天23点59分59秒
    File dir = new File("/Users/xutengqiang/maplecloudy/workspace/maplecloudy-platform-demo");
//    Git.init().setGitDir(dir).setDirectory(dir.getParentFile()).call();
//    Git.cloneRepository().setURI("https://github.com/learner1992/EasyReport.git")
//    .setDirectory(new File("/Users/xutengqiang/maplecloudy/workspace/test")).call();
    
//    Git git = Git.cloneRepository()
//        .setURI("https://github.com/learner1992/EasyReport.git")
//        .setDirectory(dir)
//        .setCloneAllBranches(true)
//        .call();
//    git.
    Ref call = Git.open(dir).checkout().setName("master").call();
    CheckoutCommand command = Git.open(dir).checkout().setName("master");
    Git git = Git.open(dir);
    Repository repository = git.getRepository();
    git.archive().registerFormat("tar", new ZipArchiveFormat());
    FileOutputStream out = new FileOutputStream(
        new File("/Users/xutengqiang/maplecloudy/workspace/test1"));
    git.archive().setTree(repository.resolve("master")).setFormat("tar")
        .setOutputStream(out).call();
    List<Object> list = Lists.newArrayList();
    list.add("a");
    list.add("b");
    list.add("s");
    list.add("a");
    out.close();
//    CollectionUtils.filter(list, new Predicate());
    ArchiveCommand.unregisterFormat("tar");
  }
  
  private static final class ZipArchiveFormat
      implements Format<ZipOutputStream> {
    
    @Override
    public ZipOutputStream createArchiveOutputStream(OutputStream s) {
      return new ZipOutputStream(s);
    }
    
    @Override
    public void putEntry(ZipOutputStream out, ObjectId tree, String path,
        FileMode mode, ObjectLoader loader) throws IOException {
      // loader is null for directories...
      if (loader != null) {
        ZipEntry entry = new ZipEntry(path);
        
        if (tree instanceof RevCommit) {
          long t = ((RevCommit) tree).getCommitTime() * 1000L;
          entry.setTime(t);
        }
        
        out.putNextEntry(entry);
        out.write(loader.getBytes());
        out.closeEntry();
      }
    }
    
    @Override
    public Iterable<String> suffixes() {
      return Collections.singleton(".mzip");
    }
    
    @Override
    public ZipOutputStream createArchiveOutputStream(OutputStream s,
        Map<String,Object> o) {
      return new ZipOutputStream(s);
    }

    @Override
    public void putEntry(ZipOutputStream out, String path, FileMode mode,
        ObjectLoader loader) throws IOException {
      // TODO Auto-generated method stub
      
    }
  }
}
