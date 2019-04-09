package config;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.GroupMappingServiceProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A simple fakes implementation of {@link GroupMappingServiceProvider} that
 * exec's the <code>groups</code> shell command to fetch the group memberships
 * of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class FakeUnixGroupsMapping implements GroupMappingServiceProvider {
  
  /**
   * Returns list of groups for a user
   *
   * @param userName
   *          get groups for this user
   * @return list of groups for a given user
   */
  public List<String> getGroups(String userName) throws IOException {
    return Collections.singletonList(userName);
  }
  
  /**
   * Caches groups, no need to do that for this provider
   */
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }
  
  /**
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups
   *          unused
   */
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
  
}
