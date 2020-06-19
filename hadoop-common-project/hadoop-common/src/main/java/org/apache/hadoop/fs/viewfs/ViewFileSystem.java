/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE_DEFAULT;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS_DEFAULT;
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

/**
 * ViewFileSystem (extends the FileSystem interface) implements a client-side
 * mount table. Its spec and implementation is identical to {@link ViewFs}.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFileSystem extends FileSystem {

  private static final Path ROOT_PATH = new Path(Path.SEPARATOR);

  static AccessControlException readOnlyMountTable(final String operation,
      final String p) {
    return new AccessControlException(
        "InternalDir of ViewFileSystem is readonly, operation " + operation +
            " not permitted on path " + p + ".");
  }
  static AccessControlException readOnlyMountTable(final String operation,
      final Path p) {
    return readOnlyMountTable(operation, p.toString());
  }

  /**
   * Gets file system creator instance.
   */
  protected FsGetter fsGetter() {
    return new FsGetter();
  }

  /**
   * Caching children filesystems. HADOOP-15565.
   */
  static class InnerCache {
    private Map<Key, FileSystem> map = new HashMap<>();
    private FsGetter fsCreator;

    InnerCache(FsGetter fsCreator) {
      this.fsCreator = fsCreator;
    }

    FileSystem get(URI uri, Configuration config) throws IOException {
      Key key = new Key(uri);
      if (map.get(key) == null) {
        FileSystem fs = fsCreator.getNewInstance(uri, config);
        map.put(key, fs);
        return fs;
      } else {
        return map.get(key);
      }
    }

    void closeAll() {
      for (FileSystem fs : map.values()) {
        try {
          fs.close();
        } catch (IOException e) {
          LOG.info("Fail closing ViewFileSystem's child filesystem " + fs, e);
        }
      }
    }

    InnerCache unmodifiableCache() {
      map = Collections.unmodifiableMap(map);
      return this;
    }

    /**
     * All the cached instances share the same UGI so there is no need to have a
     * URI in the Key. Make the Key simple with just the scheme and authority.
     */
    private static class Key {
      private final String scheme;
      private final String authority;

      Key(URI uri) {
        scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        authority =
            uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
      }

      @Override
      public int hashCode() {
        return Objects.hash(scheme, authority);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof Key) {
          Key that = (Key) obj;
          return this.scheme.equals(that.scheme) && this.authority
              .equals(that.authority);
        }
        return false;
      }
    }
  }

  /**
   * MountPoint representation built from the configuration.
   */
  public static class MountPoint {

    /**
     * The mounted on path location.
     */
    private final Path mountedOnPath;

    /**
     * Array of target FileSystem URIs.
     */
    private final URI[] targetFileSystemURIs;

    MountPoint(Path srcPath, URI[] targetFs) {
      mountedOnPath = srcPath;
      targetFileSystemURIs = targetFs;
    }

    public Path getMountedOnPath() {
      return mountedOnPath;
    }

    public URI[] getTargetFileSystemURIs() {
      return targetFileSystemURIs;
    }
  }

  final long creationTime; // of the the mount table
  final UserGroupInformation ugi; // the user/group of user who created mtable
  private URI myUri;
  private Path workingDir;
  Configuration config;
  InodeTree<FileSystem> fsState;  // the fs state; ie the mount table
  Path homeDir = null;
  private boolean enableInnerCache = false;
  private InnerCache cache;
  // Default to rename within same mountpoint
  private RenameStrategy renameStrategy = RenameStrategy.SAME_MOUNTPOINT;
  /**
   * Make the path Absolute and get the path-part of a pathname.
   * Checks that URI matches this file system 
   * and that the path-part is a valid name.
   * 
   * @param p path
   * @return path-part of the Path p
   */
  String getUriPath(final Path p) {
    checkPath(p);
    return makeAbsolute(p).toUri().getPath();
  }
  
  private Path makeAbsolute(final Path f) {
    return f.isAbsolute() ? f : new Path(workingDir, f);
  }
  
  /**
   * This is the  constructor with the signature needed by
   * {@link FileSystem#createFileSystem(URI, Configuration)}
   * 
   * After this constructor is called initialize() is called.
   * @throws IOException 
   */
  public ViewFileSystem() throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    creationTime = Time.now();
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>viewfs</code>
   */
  @Override
  public String getScheme() {
    return FsConstants.VIEWFS_SCHEME;
  }

  /**
   * Called after a new FileSystem instance is constructed.
   * @param theUri a uri whose authority section names the host, port, etc. for
   *        this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(final URI theUri, final Configuration conf)
      throws IOException {
    super.initialize(theUri, conf);
    setConf(conf);
    config = conf;
    enableInnerCache = config.getBoolean(CONFIG_VIEWFS_ENABLE_INNER_CACHE,
        CONFIG_VIEWFS_ENABLE_INNER_CACHE_DEFAULT);
    FsGetter fsGetter = fsGetter();
    final InnerCache innerCache = new InnerCache(fsGetter);
    // Now build  client side view (i.e. client side mount table) from config.
    final String authority = theUri.getAuthority();
    try {
      myUri = new URI(getScheme(), authority, "/", null, null);
      fsState = new InodeTree<FileSystem>(conf, authority) {
        @Override
        protected FileSystem getTargetFileSystem(final URI uri)
          throws URISyntaxException, IOException {
            FileSystem fs;
            if (enableInnerCache) {
              fs = innerCache.get(uri, config);
            } else {
              fs = fsGetter.get(uri, config);
            }
            return new ChRootedFileSystem(fs, uri);
        }

        @Override
        protected FileSystem getTargetFileSystem(final INodeDir<FileSystem> dir)
          throws URISyntaxException {
          return new InternalDirOfViewFs(dir, creationTime, ugi, myUri, config);
        }

        @Override
        protected FileSystem getTargetFileSystem(final String settings,
            final URI[] uris) throws URISyntaxException, IOException {
          return NflyFSystem.createFileSystem(uris, config, settings,
              fsGetter);
        }
      };
      workingDir = this.getHomeDirectory();
      renameStrategy = RenameStrategy.valueOf(
          conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
              RenameStrategy.SAME_MOUNTPOINT.toString()));
    } catch (URISyntaxException e) {
      throw new IOException("URISyntax exception: " + theUri);
    }

    if (enableInnerCache) {
      // All fs instances are created and cached on startup. The cache is
      // readonly after the initialize() so the concurrent access of the cache
      // is safe.
      cache = innerCache.unmodifiableCache();
    }
  }

  /**
   * Convenience Constructor for apps to call directly
   * @param theUri which must be that of ViewFileSystem
   * @param conf
   * @throws IOException
   */
  ViewFileSystem(final URI theUri, final Configuration conf)
      throws IOException {
    this();
    initialize(theUri, conf);
  }
  
  /**
   * Convenience Constructor for apps to call directly
   * @param conf
   * @throws IOException
   */
  public ViewFileSystem(final Configuration conf) throws IOException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  @Override
  public URI getUri() {
    return myUri;
  }
  
  @Override
  public Path resolvePath(final Path f) throws IOException {
    final InodeTree.ResolveResult<FileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);
  }
  
  @Override
  public Path getHomeDirectory() {
    if (homeDir == null) {
      String base = fsState.getHomeDirPrefixValue();
      if (base == null) {
        base = "/user";
      }
      homeDir = (base.equals("/") ? 
          this.makeQualified(new Path(base + ugi.getShortUserName())):
          this.makeQualified(new Path(base + "/" + ugi.getShortUserName())));
    }
    return homeDir;
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    getUriPath(new_dir); // this validates the path
    workingDir = makeAbsolute(new_dir);
  }
  
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.append(res.remainingPath, bufferSize, progress);
  }
  
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createNonRecursive(res.remainingPath,
        permission, flags, bufferSize, replication, blockSize, progress);
  }
  
  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.create(res.remainingPath, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  
  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws AccessControlException, FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw readOnlyMountTable("delete", f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }
  
  @Override
  @SuppressWarnings("deprecation")
  public boolean delete(final Path f)
      throws AccessControlException, FileNotFoundException, IOException {
    return delete(f, true);
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus fs, 
      long start, long len) throws IOException {
    final InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(fs.getPath()), true);
    return res.targetFileSystem.getFileBlockLocations(
        new ViewFsFileStatus(fs, res.remainingPath), start, len);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public FileChecksum getFileChecksum(final Path f, final long length)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath, length);
  }

  private static FileStatus fixFileStatus(FileStatus orig,
      Path qualified) throws IOException {
    // FileStatus#getPath is a fully qualified path relative to the root of
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.

    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwner lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFileSystemFileStatus that
    // works around.
    if ("file".equals(orig.getPath().toUri().getScheme())) {
      orig = wrapLocalFileStatus(orig, qualified);
    }

    orig.setPath(qualified);
    return orig;
  }

  private static FileStatus wrapLocalFileStatus(FileStatus orig,
      Path qualified) {
    return orig instanceof LocatedFileStatus
        ? new ViewFsLocatedFileStatus((LocatedFileStatus)orig, qualified)
        : new ViewFsFileStatus(orig, qualified);
  }

  /**
   * {@inheritDoc}
   *
   * If the given path is a symlink(mount link), the path will be resolved to a
   * target path and it will get the resolved path's FileStatus object. It will
   * not be represented as a symlink and isDirectory API returns true if the
   * resolved path is a directory, false otherwise.
   */
  @Override
  public FileStatus getFileStatus(final Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return fixFileStatus(status, this.makeQualified(f));
  }
  
  @Override
  public void access(Path path, FsAction mode) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.access(res.remainingPath, mode);
  }

  /**
   * {@inheritDoc}
   *
   * Note: listStatus on root("/") considers listing from fallbackLink if
   * available. If the same directory name is present in configured mount path
   * as well as in fallback link, then only the configured mount path will be
   * listed in the returned result.
   *
   * If any of the the immediate children of the given path f is a symlink(mount
   * link), the returned FileStatus object of that children would be represented
   * as a symlink. It will not be resolved to the target path and will not get
   * the target path FileStatus object. The target path will be available via
   * getSymlink on that children's FileStatus object. Since it represents as
   * symlink, isDirectory on that children's FileStatus will return false.
   *
   * If you want to get the FileStatus of target path for that children, you may
   * want to use GetFileStatus API with that children's symlink path. Please see
   * {@link ViewFileSystem#getFileStatus(Path f)}
   */
  @Override
  public FileStatus[] listStatus(final Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    FileStatus[] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      int i = 0;
      for (FileStatus status : statusLst) {
          statusLst[i++] = fixFileStatus(status,
              getChrootedPath(res, status, f));
      }
    }
    return statusLst;
  }

  @Override
  public RemoteIterator<LocatedFileStatus>listLocatedStatus(final Path f,
      final PathFilter filter) throws FileNotFoundException, IOException {
    final InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    final RemoteIterator<LocatedFileStatus> statusIter =
        res.targetFileSystem.listLocatedStatus(res.remainingPath);

    if (res.isInternalDir()) {
      return statusIter;
    }

    return new RemoteIterator<LocatedFileStatus>() {
      @Override
      public boolean hasNext() throws IOException {
        return statusIter.hasNext();
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        final LocatedFileStatus status = statusIter.next();
        return (LocatedFileStatus)fixFileStatus(status,
            getChrootedPath(res, status, f));
      }
    };
  }

  private Path getChrootedPath(InodeTree.ResolveResult<FileSystem> res,
      FileStatus status, Path f) throws IOException {
    final String suffix;
    if (res.targetFileSystem instanceof ChRootedFileSystem) {
      suffix = ((ChRootedFileSystem)res.targetFileSystem)
          .stripOutRoot(status.getPath());
    } else { // nfly
      suffix = ((NflyFSystem.NflyStatus)status).stripRoot();
    }
    return this.makeQualified(
        suffix.length() == 0 ? f : new Path(res.resolvedPath, suffix));
  }

  @Override
  public boolean mkdirs(Path dir) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(dir), false);
    return res.targetFileSystem.mkdirs(res.remainingPath);
  }

  @Override
  public boolean mkdirs(final Path dir, final FsPermission permission)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = 
        fsState.resolve(getUriPath(dir), false);
    return res.targetFileSystem.mkdirs(res.remainingPath, permission);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws AccessControlException, FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    // passing resolveLastComponet as false to catch renaming a mount point to 
    // itself. We need to catch this as an internal operation and fail.
    InodeTree.ResolveResult<FileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw readOnlyMountTable("rename", src);
    }
      
    InodeTree.ResolveResult<FileSystem> resDst = 
      fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
          throw readOnlyMountTable("rename", dst);
    }

    URI srcUri = resSrc.targetFileSystem.getUri();
    URI dstUri = resDst.targetFileSystem.getUri();

    verifyRenameStrategy(srcUri, dstUri,
        resSrc.targetFileSystem == resDst.targetFileSystem, renameStrategy);

    if (resSrc.targetFileSystem instanceof ChRootedFileSystem &&
        resDst.targetFileSystem instanceof ChRootedFileSystem) {
      ChRootedFileSystem srcFS = (ChRootedFileSystem) resSrc.targetFileSystem;
      ChRootedFileSystem dstFS = (ChRootedFileSystem) resDst.targetFileSystem;
      return srcFS.getMyFs().rename(srcFS.fullPath(resSrc.remainingPath),
          dstFS.fullPath(resDst.remainingPath));
    } else {
      return resSrc.targetFileSystem.rename(resSrc.remainingPath, resDst.remainingPath);
    }
  }

  static void verifyRenameStrategy(URI srcUri, URI dstUri,
      boolean isSrcDestSame, ViewFileSystem.RenameStrategy renameStrategy)
      throws IOException {
    switch (renameStrategy) {
    case SAME_FILESYSTEM_ACROSS_MOUNTPOINT:
      if (srcUri.getAuthority() != null) {
        if (!(srcUri.getScheme().equals(dstUri.getScheme()) && srcUri
            .getAuthority().equals(dstUri.getAuthority()))) {
          throw new IOException("Renames across Mount points not supported");
        }
      }

      break;
    case SAME_TARGET_URI_ACROSS_MOUNTPOINT:
      // Alternate 2: Rename across mountpoints with same target.
      // i.e. Rename across alias mountpoints.
      //
      // Note we compare the URIs. the URIs include the link targets.
      // hence we allow renames across mount links as long as the mount links
      // point to the same target.
      if (!srcUri.equals(dstUri)) {
        throw new IOException("Renames across Mount points not supported");
      }

      break;
    case SAME_MOUNTPOINT:
      //
      // Alternate 3 : renames ONLY within the the same mount links.
      //
      if (!isSrcDestSame) {
        throw new IOException("Renames across Mount points not supported");
      }
      break;
    default:
      throw new IllegalArgumentException ("Unexpected rename strategy");
    }
  }

  @Override
  public boolean truncate(final Path f, final long newLength)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.truncate(res.remainingPath, newLength);
  }
  
  @Override
  public void setOwner(final Path f, final String username,
      final String groupname) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(final Path f, final FsPermission permission)
      throws AccessControlException, FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
  }

  @Override
  public boolean setReplication(final Path f, final short replication)
      throws AccessControlException, FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(final Path f, final long mtime, final long atime)
      throws AccessControlException, FileNotFoundException, IOException {
    InodeTree.ResolveResult<FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.modifyAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.removeAclEntries(res.remainingPath, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeDefaultAcl(res.remainingPath);
  }

  @Override
  public void removeAcl(Path path)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.removeAcl(res.remainingPath);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setAcl(res.remainingPath, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getAclStatus(res.remainingPath);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setXAttr(res.remainingPath, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttr(res.remainingPath, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.getXAttrs(res.remainingPath, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(path), true);
    return res.targetFileSystem.listXAttrs(res.remainingPath);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.removeXAttr(res.remainingPath, name);
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum) { 
    List<InodeTree.MountPoint<FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.MountPoint<FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setVerifyChecksum(verifyChecksum);
    }
  }
  
  @Override
  public long getDefaultBlockSize() {
    throw new NotInMountpointException("getDefaultBlockSize");
  }

  @Override
  public short getDefaultReplication() {
    throw new NotInMountpointException("getDefaultReplication");
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    throw new NotInMountpointException("getServerDefaults");
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    try {
      InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultBlockSize(res.remainingPath);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(f, "getDefaultBlockSize"); 
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    try {
      InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultReplication(res.remainingPath);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(f, "getDefaultReplication"); 
    }
  }

  @Override
  public FsServerDefaults getServerDefaults(Path f) throws IOException {
    try {
      InodeTree.ResolveResult<FileSystem> res =
          fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getServerDefaults(res.remainingPath);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(f, "getServerDefaults");
    }
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getContentSummary(res.remainingPath);
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getQuotaUsage(res.remainingPath);
  }

  @Override
  public void setWriteChecksum(final boolean writeChecksum) { 
    List<InodeTree.MountPoint<FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.MountPoint<FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setWriteChecksum(writeChecksum);
    }
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    List<InodeTree.MountPoint<FileSystem>> mountPoints =
        fsState.getMountPoints();
    Set<FileSystem> children = new HashSet<FileSystem>();
    for (InodeTree.MountPoint<FileSystem> mountPoint : mountPoints) {
      FileSystem targetFs = mountPoint.target.targetFileSystem;
      children.addAll(Arrays.asList(targetFs.getChildFileSystems()));
    }
    return children.toArray(new FileSystem[]{});
  }
  
  public MountPoint[] getMountPoints() {
    List<InodeTree.MountPoint<FileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    MountPoint[] result = new MountPoint[mountPoints.size()];
    for ( int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new MountPoint(new Path(mountPoints.get(i).src),
          mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    return res.targetFileSystem.createSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.renameSnapshot(res.remainingPath, snapshotOldName,
        snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(path),
        true);
    res.targetFileSystem.deleteSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void satisfyStoragePolicy(Path src) throws IOException {
    InodeTree.ResolveResult<FileSystem> res =
        fsState.resolve(getUriPath(src), true);
    res.targetFileSystem.satisfyStoragePolicy(res.remainingPath);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(src),
        true);
    res.targetFileSystem.setStoragePolicy(res.remainingPath, policyName);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(src),
        true);
    res.targetFileSystem.unsetStoragePolicy(res.remainingPath);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(getUriPath(src),
        true);
    return res.targetFileSystem.getStoragePolicy(res.remainingPath);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    Collection<BlockStoragePolicySpi> allPolicies = new HashSet<>();
    for (FileSystem fs : getChildFileSystems()) {
      try {
        Collection<? extends BlockStoragePolicySpi> policies =
            fs.getAllStoragePolicies();
        allPolicies.addAll(policies);
      } catch (UnsupportedOperationException e) {
        // ignored
      }
    }
    return allPolicies;
  }

  /**
   * Get the trash root directory for current user when the path
   * specified is deleted.
   *
   * @param path the trash root of the path to be determined.
   * @return the trash root path.
   */
  @Override
  public Path getTrashRoot(Path path) {
    try {
      InodeTree.ResolveResult<FileSystem> res =
          fsState.resolve(getUriPath(path), true);
      return res.targetFileSystem.getTrashRoot(res.remainingPath);
    } catch (Exception e) {
      throw new NotInMountpointException(path, "getTrashRoot");
    }
  }

  /**
   * Get all the trash roots for current user or all users.
   *
   * @param allUsers return trash roots for all users if true.
   * @return all Trash root directories.
   */
  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    List<FileStatus> trashRoots = new ArrayList<>();
    for (FileSystem fs : getChildFileSystems()) {
      trashRoots.addAll(fs.getTrashRoots(allUsers));
    }
    return trashRoots;
  }

  @Override
  public FsStatus getStatus() throws IOException {
    return getStatus(null);
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    if (p == null) {
      p = InodeTree.SlashPath;
    }
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(
        getUriPath(p), true);
    return res.targetFileSystem.getStatus(p);
  }

  /**
   * Return the total size of all files under "/", if {@link
   * Constants#CONFIG_VIEWFS_LINK_MERGE_SLASH} is supported and is a valid
   * mount point. Else, throw NotInMountpointException.
   *
   * @throws IOException
   */
  @Override
  public long getUsed() throws IOException {
    InodeTree.ResolveResult<FileSystem> res = fsState.resolve(
        getUriPath(InodeTree.SlashPath), true);
    if (res.isInternalDir()) {
      throw new NotInMountpointException(InodeTree.SlashPath, "getUsed");
    } else {
      return res.targetFileSystem.getUsed();
    }
  }

  @Override
  public Path getLinkTarget(Path path) throws IOException {
    InodeTree.ResolveResult<FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(path), true);
    } catch (FileNotFoundException e) {
      throw new NotInMountpointException(path, "getLinkTarget");
    }
    return res.targetFileSystem.getLinkTarget(res.remainingPath);
  }

  /**
   * Reject the concat operation; forward the rest to the viewed FS.
   * @param path path to query the capability of.
   * @param capability string to query the stream support for.
   * @return the capability
   * @throws IOException if there is no resolved FS, or it raises an IOE.
   */
  @Override
  public boolean hasPathCapability(Path path, String capability)
      throws IOException {
    final Path p = makeQualified(path);
    switch (validatePathCapabilityArgs(p, capability)) {
    case CommonPathCapabilities.FS_CONCAT:
      // concat is not supported, as it may be invoked across filesystems.
      return false;
    default:
      // no break
    }
    // otherwise, check capabilities of mounted FS.
    try {
      InodeTree.ResolveResult<FileSystem> res
          = fsState.resolve(getUriPath(p), true);
      return res.targetFileSystem.hasPathCapability(res.remainingPath,
          capability);
    } catch (FileNotFoundException e) {
      // no mount point, nothing will work.
      throw new NotInMountpointException(p, "hasPathCapability");
    }
  }

  /**
   * An instance of this class represents an internal dir of the viewFs
   * that is internal dir of the mount table.
   * It is a read only mount tables and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends FileSystem {
    final InodeTree.INodeDir<FileSystem>  theInternalDir;
    final long creationTime; // of the the mount table
    final UserGroupInformation ugi; // the user/group of user who created mtable
    final URI myUri;
    final boolean showMountLinksAsSymlinks;
    
    public InternalDirOfViewFs(final InodeTree.INodeDir<FileSystem> dir,
        final long cTime, final UserGroupInformation ugi, URI uri,
        Configuration config) throws URISyntaxException {
      myUri = uri;
      try {
        initialize(myUri, config);
      } catch (IOException e) {
        throw new RuntimeException("Cannot occur");
      }
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
      showMountLinksAsSymlinks = config
          .getBoolean(CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS,
              CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS_DEFAULT);
    }

    static private void checkPathIsSlash(final Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new IOException(
            "Internal implementation error: expected file name to be /");
      }
    }
    
    @Override
    public URI getUri() {
      return myUri;
    }

    @Override
    public Path getWorkingDirectory() {
      throw new RuntimeException(
          "Internal impl error: getWorkingDir should not have been called");
    }

    @Override
    public void setWorkingDirectory(final Path new_dir) {
      throw new RuntimeException(
          "Internal impl error: getWorkingDir should not have been called");
    }

    @Override
    public FSDataOutputStream append(final Path f, final int bufferSize,
        final Progressable progress) throws IOException {
      throw readOnlyMountTable("append", f);
    }

    @Override
    public FSDataOutputStream create(final Path f,
        final FsPermission permission, final boolean overwrite,
        final int bufferSize, final short replication, final long blockSize,
        final Progressable progress) throws AccessControlException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public boolean delete(final Path f, final boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public boolean delete(final Path f)
        throws AccessControlException, IOException {
      return delete(f, true);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(final FileStatus fs,
        final long start, final long len) throws
        FileNotFoundException, IOException {
      checkPathIsSlash(fs.getPath());
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileChecksum getFileChecksum(final Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      checkPathIsSlash(f);
      return new FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_555, ugi.getShortUserName(), ugi.getPrimaryGroupName(),
          new Path(theInternalDir.fullPath).makeQualified(
              myUri, ROOT_PATH));
    }
    

    @Override
    public FileStatus[] listStatus(Path f) throws AccessControlException,
        FileNotFoundException, IOException {
      checkPathIsSlash(f);
      FileStatus[] fallbackStatuses = listStatusForFallbackLink();
      FileStatus[] result = new FileStatus[theInternalDir.getChildren().size()];
      int i = 0;
      for (Entry<String, INode<FileSystem>> iEntry :
          theInternalDir.getChildren().entrySet()) {
        INode<FileSystem> inode = iEntry.getValue();
        Path path = new Path(inode.fullPath).makeQualified(myUri, null);
        if (inode.isLink()) {
          INodeLink<FileSystem> link = (INodeLink<FileSystem>) inode;

          if (showMountLinksAsSymlinks) {
            // To maintain backward compatibility, with default option(showing
            // mount links as symlinks), we will represent target link as
            // symlink and rest other properties are belongs to mount link only.
            result[i++] =
                new FileStatus(0, false, 0, 0, creationTime, creationTime,
                    PERMISSION_555, ugi.getShortUserName(),
                    ugi.getPrimaryGroupName(), link.getTargetLink(),
                    path);
            continue;
          }

          //  We will represent as non-symlinks. Here it will show target
          //  directory/file properties like permissions, isDirectory etc on
          //  mount path. The path will be a mount link path and isDirectory is
          //  true if target is dir, otherwise false.
          String linkedPath = link.getTargetFileSystem().getUri().getPath();
          if ("".equals(linkedPath)) {
            linkedPath = "/";
          }
          try {
            FileStatus status =
                ((ChRootedFileSystem)link.getTargetFileSystem())
                .getMyFs().getFileStatus(new Path(linkedPath));
            result[i++] = new FileStatus(status.getLen(), status.isDirectory(),
                status.getReplication(), status.getBlockSize(),
                status.getModificationTime(), status.getAccessTime(),
                status.getPermission(), status.getOwner(), status.getGroup(),
                null, path);
          } catch (FileNotFoundException ex) {
            LOG.warn("Cannot get one of the children's(" + path
                + ")  target path(" + link.getTargetFileSystem().getUri()
                + ") file status.", ex);
            throw ex;
          }
        } else {
          result[i++] = new FileStatus(0, true, 0, 0,
            creationTime, creationTime, PERMISSION_555,
            ugi.getShortUserName(), ugi.getPrimaryGroupName(),
              path);
        }
      }
      if (fallbackStatuses.length > 0) {
        return consolidateFileStatuses(fallbackStatuses, result);
      } else {
        return result;
      }
    }

    private FileStatus[] consolidateFileStatuses(FileStatus[] fallbackStatuses,
        FileStatus[] mountPointStatuses) {
      ArrayList<FileStatus> result = new ArrayList<>();
      Set<String> pathSet = new HashSet<>();
      for (FileStatus status : mountPointStatuses) {
        result.add(status);
        pathSet.add(status.getPath().getName());
      }
      for (FileStatus status : fallbackStatuses) {
        if (!pathSet.contains(status.getPath().getName())) {
          result.add(status);
        }
      }
      return result.toArray(new FileStatus[0]);
    }

    private FileStatus[] listStatusForFallbackLink() throws IOException {
      if (theInternalDir.isRoot() &&
          theInternalDir.getFallbackLink() != null) {
        FileSystem linkedFs =
            theInternalDir.getFallbackLink().getTargetFileSystem();
        // Fallback link is only applicable for root
        FileStatus[] statuses = linkedFs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
          // Fix the path back to viewfs scheme
          status.setPath(
              new Path(myUri.toString(), status.getPath().getName()));
        }
        return statuses;
      } else {
        return new FileStatus[0];
      }
    }

    @Override
    public boolean mkdirs(Path dir, FsPermission permission)
        throws AccessControlException, FileAlreadyExistsException {
      if (theInternalDir.isRoot() && dir == null) {
        throw new FileAlreadyExistsException("/ already exits");
      }
      // Note dir starts with /
      if (theInternalDir.getChildren().containsKey(
          dir.toString().substring(1))) {
        return true; // this is the stupid semantics of FileSystem
      }
      throw readOnlyMountTable("mkdirs",  dir);
    }

    @Override
    public boolean mkdirs(Path dir)
        throws AccessControlException, FileAlreadyExistsException {
      return mkdirs(dir, null);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize)
        throws AccessControlException, FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public boolean rename(Path src, Path dst) throws AccessControlException,
        IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
      throw readOnlyMountTable("truncate", f);
    }

    @Override
    public void setOwner(Path f, String username, String groupname)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(Path f, FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public boolean setReplication(Path f, short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(Path f, long mtime, long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
      // Noop for viewfs
    }

    @Override
    public FsServerDefaults getServerDefaults(Path f) throws IOException {
      throw new NotInMountpointException(f, "getServerDefaults");
    }
    
    @Override
    public long getDefaultBlockSize(Path f) {
      throw new NotInMountpointException(f, "getDefaultBlockSize");
    }

    @Override
    public short getDefaultReplication(Path f) {
      throw new NotInMountpointException(f, "getDefaultReplication");
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("modifyAclEntries", path);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAclEntries", path);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeDefaultAcl", path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeAcl", path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setAcl", path);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
      checkPathIsSlash(path);
      return new AclStatus.Builder().owner(ugi.getShortUserName())
          .group(ugi.getPrimaryGroupName())
          .addEntries(AclUtil.getMinimalAcl(PERMISSION_555))
          .stickyBit(false).build();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
        EnumSet<XAttrSetFlag> flag) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("setXAttr", path);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
      throw new NotInMountpointException(path, "getXAttr");
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
      throw new NotInMountpointException(path, "getXAttrs");
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
        throws IOException {
      throw new NotInMountpointException(path, "getXAttrs");
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
      throw new NotInMountpointException(path, "listXAttrs");
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("removeXAttr", path);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("createSnapshot", path);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
        String snapshotNewName) throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("renameSnapshot", path);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
        throws IOException {
      checkPathIsSlash(path);
      throw readOnlyMountTable("deleteSnapshot", path);
    }

    @Override
    public QuotaUsage getQuotaUsage(Path f) throws IOException {
      throw new NotInMountpointException(f, "getQuotaUsage");
    }

    @Override
    public void satisfyStoragePolicy(Path src) throws IOException {
      checkPathIsSlash(src);
      throw readOnlyMountTable("satisfyStoragePolicy", src);
    }

    @Override
    public void setStoragePolicy(Path src, String policyName)
        throws IOException {
      checkPathIsSlash(src);
      throw readOnlyMountTable("setStoragePolicy", src);
    }

    @Override
    public void unsetStoragePolicy(Path src) throws IOException {
      checkPathIsSlash(src);
      throw readOnlyMountTable("unsetStoragePolicy", src);
    }

    @Override
    public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
      throw new NotInMountpointException(src, "getStoragePolicy");
    }

    @Override
    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
        throws IOException {
      Collection<BlockStoragePolicySpi> allPolicies = new HashSet<>();
      for (FileSystem fs : getChildFileSystems()) {
        try {
          Collection<? extends BlockStoragePolicySpi> policies =
              fs.getAllStoragePolicies();
          allPolicies.addAll(policies);
        } catch (UnsupportedOperationException e) {
          // ignored
        }
      }
      return allPolicies;
    }
  }

  enum RenameStrategy {
    SAME_MOUNTPOINT, SAME_TARGET_URI_ACROSS_MOUNTPOINT,
    SAME_FILESYSTEM_ACROSS_MOUNTPOINT
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (enableInnerCache && cache != null) {
      cache.closeAll();
    }
  }
}
