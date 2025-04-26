use crate::net::tcp::ConnectStream;
use crate::net::tcp::Listener;
use anyhow::Result;
use ibverbs::{
    CompletionQueue, Context as IbContext, MemoryRegion, ProtectionDomain, QueuePair,
    QueuePairEndpoint, ibv_qp_type,
};
use ibverbs_sys::*;
use serde_json;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::ptr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Turmoil doesn't support binding to a specific IP address
// https://github.com/tokio-rs/turmoil/blob/4098729305f7ba31131f8dde957443408c3ca306/src/net/tcp/listener.rs#L36
// So we need to bind to 0.0.0.0, and this enum helps us identify
// whether the host name is a turmoil name or a regular name
#[derive(Debug, Clone)]
pub enum HostName {
    TurmoilName(String),
    RegularName(String),
}

impl HostName {
    pub fn get_name(&self) -> &str {
        match self {
            HostName::TurmoilName(name) => name,
            HostName::RegularName(name) => name,
        }
    }
    pub fn is_turmoil(&self) -> bool {
        matches!(self, HostName::TurmoilName(_))
    }
}

const IB_SERVER_PORT: u16 = 5555;
pub const MIN_WR_ID: u64 = 1000;

/// Represents an InfiniBand socket for RDMA communication
/// This socket can handle multiple remote connections
/// by creating multiple queue pairs
pub struct IBSocket<L: Listener> {
    context: &'static IbContext,
    pd: &'static ProtectionDomain<'static>,
    cq: &'static CompletionQueue<'static>,
    qps: HashMap<String, QueuePair<'static>>,
    id: u64,
    _listener: PhantomData<L>,
}

impl<L: Listener> IBSocket<L> {
    /// Creates a new InfiniBand socket and binds a TCP server
    pub fn new() -> Result<Self> {
        let context = Box::new(
            ibverbs::devices()?
                .get(0)
                .ok_or(anyhow::anyhow!("No available devices"))?
                .open()?,
        );
        let context_ref = Box::leak(context);
        let pd = Box::new(context_ref.alloc_pd()?);
        let cq = Box::new(context_ref.create_cq(16, 0)?);
        Ok(IBSocket {
            context: context_ref,
            pd: Box::leak(pd),
            cq: Box::leak(cq),
            qps: HashMap::new(),
            id: MIN_WR_ID,
            _listener: PhantomData,
        })
    }

    pub fn is_connected(&self, remote_addr: &str) -> bool {
        self.qps.contains_key(remote_addr)
    }

    /// Helper: send a JSON-serializable message over a TCP stream
    async fn send_json<S: AsyncWriteExt + Unpin, T: serde::Serialize>(
        stream: &mut S,
        msg: &T,
    ) -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        let len = data.len() as u32;
        stream.write_u32_le(len).await?;
        stream.write_all(&data).await?;
        Ok(())
    }

    /// Helper: receive a JSON message from a TCP stream
    async fn recv_json<S: AsyncReadExt + Unpin, T: serde::de::DeserializeOwned>(
        stream: &mut S,
    ) -> Result<T> {
        let len = stream.read_u32_le().await?;
        let mut buf = vec![0u8; len as usize];
        stream.read_exact(&mut buf).await?;
        Ok(serde_json::from_slice(&buf)?)
    }

    /// Connects to a remote node using TCP for handshake
    pub async fn connect(&mut self, local_node: HostName, remote_node: String) -> Result<()> {
        // Create the queue pair
        let prepared_qp = self
            .pd
            .create_qp(self.cq, self.cq, ibv_qp_type::IBV_QPT_RC)?
            .set_gid_index(0) // Used to determine the routing domain
            .allow_remote_rw()
            .set_max_send_wr(1000)
            .set_max_recv_wr(1000)
            .set_path_mtu(5) // 4096 KB
            .build()?;

        // Accept a connection from the remote node (server side)
        let listener = if local_node.is_turmoil() {
            L::bind(format!("0.0.0.0:{}", IB_SERVER_PORT)).await?
        } else {
            L::bind(format!("{}:{}", local_node.get_name(), IB_SERVER_PORT)).await?
        };
        let (mut client_stream, mut server_stream) = if *local_node.get_name() > *remote_node {
            let (mut server_stream, _peer_addr) = listener.accept().await?;

            let remote_addr = format!("{}:{}", remote_node, 5555);
            let mut client_stream = loop {
                match <<L as Listener>::Stream as ConnectStream>::connect(&remote_addr).await {
                    Ok(stream) => break stream, // success: break the loop
                    Err(e) => {
                        if e.kind() == io::ErrorKind::ConnectionRefused {
                            tracing::info!("Connection refused, retrying...");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        } else {
                            return Err(e.into()); // some other error: give up
                        }
                    }
                }
            };
            (client_stream, server_stream)
        } else {
            let remote_addr = format!("{}:{}", remote_node, 5555);
            let mut client_stream = loop {
                match <<L as Listener>::Stream as ConnectStream>::connect(&remote_addr).await {
                    Ok(stream) => break stream, // success: break the loop
                    Err(e) => {
                        if e.kind() == io::ErrorKind::ConnectionRefused {
                            tracing::info!("Connection refused, retrying...");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        } else {
                            return Err(e.into()); // some other error: give up
                        }
                    }
                }
            };

            let (mut server_stream, _peer_addr) = listener.accept().await?;
            (client_stream, server_stream)
        };

        // Send endpoint info to remote
        let endpoint = prepared_qp.endpoint()?;
        Self::send_json(&mut client_stream, &endpoint).await?;

        // Receive endpoint info from remote
        let remote_endpoint: QueuePairEndpoint = Self::recv_json(&mut server_stream).await?;

        // Do the infiniband handshake
        let qp = prepared_qp.handshake(remote_endpoint)?;
        // Store the queue pair
        self.qps.insert(remote_node.clone(), qp);

        Ok(())
    }

    /// Registers memory region for RDMA operations
    pub fn register_memory(&self, size: usize) -> Result<MemoryRegion<u8>> {
        self.pd.allocate::<u8>(size).map_err(|e| anyhow::anyhow!(e))
    }

    pub fn post_send(&mut self, remote_node: &str, mr: &mut MemoryRegion<u8>) -> Result<u64> {
        if let Some(qp) = self.qps.get_mut(remote_node) {
            unsafe { qp.post_send(mr, ..mr.len(), self.id) }?;
        } else {
            return Err(anyhow::anyhow!("No queue pair for post send"));
        }
        self.id += 1;
        Ok(self.id - 1)
    }

    pub fn post_receive(&mut self, remote_node: &str, mr: &mut MemoryRegion<u8>) -> Result<u64> {
        if let Some(qp) = self.qps.get_mut(remote_node) {
            unsafe { qp.post_receive(mr, ..mr.len(), self.id) }?;
        } else {
            return Err(anyhow::anyhow!("No queue pair for post receive"));
        }
        self.id += 1;
        Ok(self.id - 1)
    }

    /// RDMA operation
    pub fn rdma_op(
        &mut self,
        remote_node: &str,
        lmr: &mut MemoryRegion<u8>,
        raddr: u64,
        rkey: u32,
        opcode: ::std::os::raw::c_uint,
        size: usize,
    ) -> Result<u64> {
        let mut sge = ibv_sge {
            addr: unsafe { (*lmr.mr).addr } as u64,
            length: size as u32,
            lkey: unsafe { (*lmr.mr).lkey },
        };
        let mut wr = ibv_send_wr {
            wr_id: self.id,
            next: ptr::null::<ibv_send_wr>() as *mut _,
            sg_list: &mut sge as *mut _,
            num_sge: 1,
            opcode,
            send_flags: ibv_send_flags::IBV_SEND_SIGNALED.0,
            wr: ibv_send_wr__bindgen_ty_2 {
                rdma: ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
                    remote_addr: raddr,
                    rkey: rkey,
                },
            },
            qp_type: Default::default(),
            __bindgen_anon_1: Default::default(),
            __bindgen_anon_2: Default::default(),
        };
        let mut bad_wr: *mut ibv_send_wr = ptr::null::<ibv_send_wr>() as *mut _;

        if let Some(qp) = self.qps.get_mut(remote_node) {
            let ctx = unsafe { (*qp.qp).context };
            let ops = unsafe { &mut (*ctx).ops };
            let errno = unsafe {
                ops.post_send
                    .as_mut()
                    .ok_or(anyhow::anyhow!("No post send function"))?(
                    qp.qp,
                    &mut wr as *mut _,
                    &mut bad_wr as *mut _,
                )
            };
            if errno != 0 {
                return Err(anyhow::anyhow!(format!("RDMA op failed: {}", errno)));
            }
        } else {
            return Err(anyhow::anyhow!("No queue pair for rdma read"));
        }

        self.id += 1;
        Ok(self.id - 1)
    }

    /// Reads data using RDMA
    pub fn rdma_read(
        &mut self,
        remote_node: &str,
        lmr: &mut MemoryRegion<u8>,
        raddr: u64,
        rkey: u32,
        size: usize,
    ) -> Result<u64> {
        self.rdma_op(
            remote_node,
            lmr,
            raddr,
            rkey,
            ibv_wr_opcode::IBV_WR_RDMA_READ,
            size,
        )
    }

    /// Writes data using RDMA
    pub fn rdma_write(
        &mut self,
        remote_node: &str,
        lmr: &mut MemoryRegion<u8>,
        raddr: u64,
        rkey: u32,
        size: usize,
    ) -> Result<u64> {
        self.rdma_op(
            remote_node,
            lmr,
            raddr,
            rkey,
            ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            size,
        )
    }

    pub fn poll_completions(&self, completions: &mut [ibverbs::ibv_wc]) -> Result<usize> {
        let completed = self.cq.poll(completions)?;
        Ok(completed.len())
    }

    pub fn get_completion(&self, wr_id: u64) -> Result<()> {
        let mut completions = [ibverbs::ibv_wc::default(); 16];
        loop {
            let completed = self.cq.poll(&mut completions[..])?;
            if !completed.is_empty() {
                break;
            }
        }
        for completion in completions.iter() {
            if completion.wr_id() != wr_id {
                continue;
            }
            parse_completion_error(completion)?;
        }
        Ok(())
    }
}

pub fn parse_completion_error(completion: &ibverbs::ibv_wc) -> Result<()> {
    if let Some((status, err)) = completion.error() {
        // Get more detailed error information
        let status_str = match status {
            ibv_wc_status::IBV_WC_SUCCESS => "IBV_WC_SUCCESS: Success",
            ibv_wc_status::IBV_WC_LOC_LEN_ERR => "IBV_WC_LOC_LEN_ERR: Local Length Error",
            ibv_wc_status::IBV_WC_LOC_QP_OP_ERR => "IBV_WC_LOC_QP_OP_ERR: Local QP Operation Error",
            ibv_wc_status::IBV_WC_LOC_EEC_OP_ERR => {
                "IBV_WC_LOC_EEC_OP_ERR: Local EEC Operation Error"
            }
            ibv_wc_status::IBV_WC_LOC_PROT_ERR => "IBV_WC_LOC_PROT_ERR: Local Protection Error",
            ibv_wc_status::IBV_WC_WR_FLUSH_ERR => "IBV_WC_WR_FLUSH_ERR: Work Request Flush Error",
            ibv_wc_status::IBV_WC_MW_BIND_ERR => "IBV_WC_MW_BIND_ERR: Memory Window Binding Error",
            ibv_wc_status::IBV_WC_BAD_RESP_ERR => "IBV_WC_BAD_RESP_ERR: Bad Response Error",
            ibv_wc_status::IBV_WC_LOC_ACCESS_ERR => "IBV_WC_LOC_ACCESS_ERR: Local Access Error",
            ibv_wc_status::IBV_WC_REM_ACCESS_ERR => "IBV_WC_REM_ACCESS_ERR: Remote Access Error",
            ibv_wc_status::IBV_WC_REM_OP_ERR => "IBV_WC_REM_OP_ERR: Remote Operation Error",
            ibv_wc_status::IBV_WC_RETRY_EXC_ERR => "IBV_WC_RETRY_EXC_ERR: Retry Exceeded Error",
            ibv_wc_status::IBV_WC_RNR_RETRY_EXC_ERR => {
                "IBV_WC_RNR_RETRY_EXC_ERR: RNR Retry Exceeded Error"
            }
            ibv_wc_status::IBV_WC_LOC_RDD_VIOL_ERR => {
                "IBV_WC_LOC_RDD_VIOL_ERR: Local RDD Violation Error"
            }
            ibv_wc_status::IBV_WC_REM_INV_REQ_ERR => {
                "IBV_WC_REM_INV_REQ_ERR: Remote Invalid Request Error"
            }
            ibv_wc_status::IBV_WC_REM_INV_RD_REQ_ERR => {
                "IBV_WC_REM_INV_RD_REQ_ERR: Remote Invalid Read Request Error"
            }
            ibv_wc_status::IBV_WC_REM_ABORT_ERR => "IBV_WC_REM_ABORT_ERR: Remote Operation Aborted",
            ibv_wc_status::IBV_WC_INV_EECN_ERR => "IBV_WC_INV_EECN_ERR: Invalid EE Context Number",
            ibv_wc_status::IBV_WC_INV_EEC_STATE_ERR => {
                "IBV_WC_INV_EEC_STATE_ERR: Invalid EE Context State"
            }
            ibv_wc_status::IBV_WC_FATAL_ERR => "IBV_WC_FATAL_ERR: Fatal Error",
            ibv_wc_status::IBV_WC_RESP_TIMEOUT_ERR => {
                "IBV_WC_RESP_TIMEOUT_ERR: Response Timeout Error"
            }
            ibv_wc_status::IBV_WC_GENERAL_ERR => "IBV_WC_GENERAL_ERR: General Error",
            _ => "Unknown Error",
        };
        return Err(anyhow::anyhow!(
            "RDMA Op failed: {} (status: {:?}, vendor_err: {:?})",
            status_str,
            status,
            err
        ));
    }
    Ok(())
}
