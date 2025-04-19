use clap::{Arg, ArgAction, Command};
use env_logger;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use libc::ENOENT;
use log::{debug, info, warn};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tfs::client::{RpcClient, ZookeeperClient};
use tfs::net::utils::my_name;
use tokio::runtime::Runtime;
const TTL: Duration = Duration::from_secs(1); // 1 second

const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

macro_rules! create_attr {
    ($ino:expr, $size:expr) => {
        FileAttr {
            ino: $ino,
            size: $size,
            blocks: 1,
            atime: UNIX_EPOCH, // 1970-01-01 00:00:00
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    };
}

struct TFS {
    rt: Runtime,
    next_file_handle: AtomicU64,
    read_rpc_client: Option<RpcClient>, // The read client will be different, to distribute the load
    write_rpc_client: RpcClient,        // The write client will always be the head of the chain
    zookeeper_client: ZookeeperClient,
}

impl TFS {
    fn new(read_hostname: &str, write_hostname: &str) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (read_rpc_client, write_rpc_client, zookeeper_client) = rt.block_on(async {
            info!("Creating RPC client");
            let mut read_rpc_client = if read_hostname != write_hostname {
                Some(
                    RpcClient::new(my_name().unwrap(), read_hostname.to_string())
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            if let Some(read_rpc_client) = &mut read_rpc_client {
                read_rpc_client.connect_ib().await.unwrap();
            }
            let mut write_rpc_client =
                RpcClient::new(my_name().unwrap(), write_hostname.to_string())
                    .await
                    .unwrap();
            write_rpc_client.connect_ib().await.unwrap();
            info!("Creating Zookeeper client");
            // Distribute the zookeeper load by using the read hostname
            let zookeeper_client = ZookeeperClient::new(read_hostname.to_string())
                .await
                .unwrap();
            (read_rpc_client, write_rpc_client, zookeeper_client)
        });
        Self {
            rt,
            next_file_handle: AtomicU64::new(1),
            read_rpc_client,
            write_rpc_client,
            zookeeper_client,
        }
    }

    fn allocate_next_file_handle(&self) -> u64 {
        self.next_file_handle.fetch_add(1, Ordering::SeqCst)
    }
}

impl Filesystem for TFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 {
            let ino = self.rt.block_on(async {
                let name = name.to_str().unwrap();
                self.zookeeper_client.get_ino(name).await
            });
            let Ok(ino) = ino else {
                warn!("Failed to get inode for {:?}", name);
                reply.error(ENOENT);
                return;
            };
            let size = self
                .rt
                .block_on(async { self.zookeeper_client.get_size(ino).await });
            let Ok(size) = size else {
                warn!(
                    "Failed to get size for inode {}, {}",
                    ino,
                    size.err().unwrap()
                );
                reply.error(ENOENT);
                return;
            };
            reply.entry(&TTL, &create_attr!(ino, size), 0);
        } else {
            warn!(
                "lookup() called for {:?}, with invalid parent {}",
                name, parent
            );
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        info!("getattr() called for {:?}", ino);
        if ino == 1 {
            reply.attr(&TTL, &ROOT_DIR_ATTR);
        } else {
            let size = self
                .rt
                .block_on(async { self.zookeeper_client.get_size(ino).await });
            info!("getattr() got size for {:?}: {:?}", ino, size);
            let Ok(size) = size else {
                warn!(
                    "Failed to get file size for inode {}, {}",
                    ino,
                    size.err().unwrap()
                );
                reply.error(ENOENT);
                return;
            };
            reply.attr(&TTL, &create_attr!(ino, size));
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        info!("write() called with {:?} size={:?}", ino, data.len());
        if offset != 0 {
            warn!("write() called with offset != 0, which is not supported");
            reply.error(libc::EACCES);
            return;
        }

        let res = self
            .rt
            .block_on(async { self.write_rpc_client.send_put_request(ino, 0, data).await });
        let Ok(_) = res else {
            warn!("Failed to send put request");
            reply.error(libc::EACCES);
            return;
        };
        reply.written(data.len() as u32);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        info!("read() called with {:?} size={:?}", ino, size);
        if offset != 0 {
            warn!("read() called with offset != 0, which is not supported");
            reply.error(libc::EACCES);
            return;
        }

        let res = self.rt.block_on(async {
            if let Some(read_rpc_client) = &mut self.read_rpc_client {
                read_rpc_client
                    .send_get_request(ino, 0, size as usize)
                    .await
            } else {
                self.write_rpc_client
                    .send_get_request(ino, 0, size as usize)
                    .await
            }
        });
        let Ok(data) = res else {
            warn!("Failed to send get request");
            reply.error(libc::EACCES);
            return;
        };
        info!(
            "read() returned data of size {:?} {:?}",
            data.len(),
            data[..15].to_vec()
        );
        reply.data(&data);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        let mut entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
        ];

        let Ok(files) = self
            .rt
            .block_on(async { self.zookeeper_client.readdir().await })
        else {
            warn!("Failed to readdir");
            reply.error(ENOENT);
            return;
        };
        for (ino, name) in &files {
            entries.push((ino.clone(), FileType::RegularFile, name));
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn setattr(
        &mut self,
        req: &Request,
        inode: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        warn!("setattr() implementation is currently a noop");
        let size = self
            .rt
            .block_on(async { self.zookeeper_client.get_size(inode).await });
        let Ok(size) = size else {
            warn!(
                "Failed to get file size for inode {}, {}",
                inode,
                size.err().unwrap()
            );
            reply.error(ENOENT);
            return;
        };
        reply.attr(&Duration::new(0, 0), &create_attr!(inode, size));
        return;
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let file_type = mode & libc::S_IFMT as u32;

        if file_type != libc::S_IFREG as u32 {
            // TODO
            warn!(
                "mknod() implementation is incomplete. Only supports regular files. Got {:o}",
                mode
            );
            reply.error(libc::ENOSYS);
            return;
        }

        if parent != 1 {
            warn!(
                "mknod() implementation is incomplete. Only supports regular files in root directory. Got {}",
                parent
            );
            reply.error(ENOENT);
            return;
        }

        let Some(file_name) = name.to_str() else {
            warn!("file name is not valid. Got {:?}", name);
            reply.error(ENOENT);
            return;
        };

        let inode = self
            .rt
            .block_on(async { self.zookeeper_client.create_inode(file_name).await });
        let Ok(inode) = inode else {
            warn!("failed to create inode. Got {}", inode.err().unwrap());
            reply.error(ENOENT);
            return;
        };

        info!("mknod() called for {:?}", inode);
        reply.entry(&Duration::new(0, 0), &create_attr!(inode, 0), 0);
    }

    fn open(&mut self, req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        info!("open() called for {:?}", inode);
        match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags & libc::O_TRUNC != 0 {
                    warn!("open() called with O_TRUNC, but O_RDONLY is not supported");
                    reply.error(libc::EACCES);
                    return;
                }
            }
            libc::O_WRONLY => {}
            libc::O_RDWR => {}
            // Exactly one access mode flag must be specified
            _ => {
                warn!("open() called with invalid flags: {:o}", flags);
                reply.error(libc::EINVAL);
                return;
            }
        };

        reply.opened(self.allocate_next_file_handle(), 0);
    }
}

fn main() {
    env_logger::init();
    let matches = Command::new("tfs")
        .author("Tarushii Goel")
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(1)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("READ_HOSTNAME")
                .required(true)
                .index(2)
                .help("The hostname of the read server"),
        )
        .arg(
            Arg::new("WRITE_HOSTNAME")
                .required(true)
                .index(3)
                .help("The hostname of the write server"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .get_matches();
    let mountpoint = matches.get_one::<String>("MOUNT_POINT").unwrap();
    let read_hostname = matches.get_one::<String>("READ_HOSTNAME").unwrap();
    let write_hostname = matches.get_one::<String>("WRITE_HOSTNAME").unwrap();
    let mut options = vec![MountOption::FSName("tfs".to_string())];
    if matches.get_flag("auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }
    fuser::mount2(
        TFS::new(read_hostname, write_hostname),
        mountpoint,
        &options,
    )
    .unwrap();
}
