use anyhow::Result;
use ibverbs::{
    CompletionQueue, Context as IbContext, MemoryRegion, ProtectionDomain, QueuePair,
    QueuePairEndpoint, ibv_qp_type,
};
use ibverbs_sys::*;
use serde_json;
use std::collections::HashMap;
use std::ptr;
use zeromq::{Socket, SocketRecv, SocketSend};

const ZMQ_SERVER_PORT: u16 = 5555;
pub const MIN_WR_ID: u64 = 1000;

/// Represents an InfiniBand socket for RDMA communication
/// This socket can handle multiple remote connections
/// by creating multiple queue pairs
pub struct IBSocket {
    context: &'static IbContext,
    pd: &'static ProtectionDomain<'static>,
    cq: &'static CompletionQueue<'static>,
    qps: HashMap<String, QueuePair<'static>>,
    id: u64,
}

impl IBSocket {
    /// Creates a new InfiniBand socket
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
        })
    }

    pub fn is_connected(&self, remote_addr: &str) -> bool {
        self.qps.contains_key(remote_addr)
    }

    /// Connects to a remote node
    pub async fn connect(&mut self, local_node: String, remote_node: String) -> Result<()> {
        // Create the queue pair
        let prepared_qp = self
            .pd
            .create_qp(self.cq, self.cq, ibv_qp_type::IBV_QPT_RC)?
            .set_gid_index(0) // Used to determine the routing domain
            .allow_remote_rw()
            .build()?;

        // Send the endpoint information
        let mut zmq_server_socket = zeromq::RepSocket::new();
        let local_addr = format!("tcp://{}:{}", local_node, ZMQ_SERVER_PORT);
        zmq_server_socket.bind(&local_addr).await?;

        let mut zmq_client_socket = zeromq::ReqSocket::new();
        let remote_addr = format!("tcp://{}:{}", remote_node, ZMQ_SERVER_PORT);
        zmq_client_socket.connect(&remote_addr).await?;

        let msg = serde_json::to_string(&prepared_qp.endpoint()?)?;
        zmq_client_socket.send(msg.into()).await?;

        let resp = zmq_server_socket.recv().await?;

        // Do the infiniband handshake
        let endpoint_str = String::from_utf8(
            resp.get(0)
                .ok_or(anyhow::anyhow!("Invalid zmq message received"))?
                .to_vec(),
        )?;
        let endpoint: QueuePairEndpoint = serde_json::from_str(&endpoint_str)?;
        let qp = prepared_qp.handshake(endpoint)?;
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

    /// Reads data using RDMA
    pub fn rdma_read(
        &mut self,
        remote_node: &str,
        lmr: &mut MemoryRegion<u8>,
        raddr: u64,
        rkey: u32,
    ) -> Result<u64> {
        let mut sge = ibv_sge {
            addr: unsafe { (*lmr.mr).addr } as u64,
            length: unsafe { (*lmr.mr).length } as u32,
            lkey: unsafe { (*lmr.mr).lkey },
        };
        let mut wr = ibv_send_wr {
            wr_id: self.id,
            next: ptr::null::<ibv_send_wr>() as *mut _,
            sg_list: &mut sge as *mut _,
            num_sge: 1,
            opcode: ibv_wr_opcode::IBV_WR_RDMA_READ,
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
                return Err(anyhow::anyhow!(format!("RDMA read failed: {}", errno)));
            }
        } else {
            return Err(anyhow::anyhow!("No queue pair for rdma read"));
        }

        self.id += 1;
        Ok(self.id - 1)
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

            if let Some((status, err)) = completion.error() {
                // Get more detailed error information
                let status_str = match status {
                    ibv_wc_status::IBV_WC_SUCCESS => "IBV_WC_SUCCESS: Success",
                    ibv_wc_status::IBV_WC_LOC_LEN_ERR => "IBV_WC_LOC_LEN_ERR: Local Length Error",
                    ibv_wc_status::IBV_WC_LOC_QP_OP_ERR => {
                        "IBV_WC_LOC_QP_OP_ERR: Local QP Operation Error"
                    }
                    ibv_wc_status::IBV_WC_LOC_EEC_OP_ERR => {
                        "IBV_WC_LOC_EEC_OP_ERR: Local EEC Operation Error"
                    }
                    ibv_wc_status::IBV_WC_LOC_PROT_ERR => {
                        "IBV_WC_LOC_PROT_ERR: Local Protection Error"
                    }
                    ibv_wc_status::IBV_WC_WR_FLUSH_ERR => {
                        "IBV_WC_WR_FLUSH_ERR: Work Request Flush Error"
                    }
                    ibv_wc_status::IBV_WC_MW_BIND_ERR => {
                        "IBV_WC_MW_BIND_ERR: Memory Window Binding Error"
                    }
                    ibv_wc_status::IBV_WC_BAD_RESP_ERR => "IBV_WC_BAD_RESP_ERR: Bad Response Error",
                    ibv_wc_status::IBV_WC_LOC_ACCESS_ERR => {
                        "IBV_WC_LOC_ACCESS_ERR: Local Access Error"
                    }
                    ibv_wc_status::IBV_WC_REM_ACCESS_ERR => {
                        "IBV_WC_REM_ACCESS_ERR: Remote Access Error"
                    }
                    ibv_wc_status::IBV_WC_REM_OP_ERR => "IBV_WC_REM_OP_ERR: Remote Operation Error",
                    ibv_wc_status::IBV_WC_RETRY_EXC_ERR => {
                        "IBV_WC_RETRY_EXC_ERR: Retry Exceeded Error"
                    }
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
                    ibv_wc_status::IBV_WC_REM_ABORT_ERR => {
                        "IBV_WC_REM_ABORT_ERR: Remote Operation Aborted"
                    }
                    ibv_wc_status::IBV_WC_INV_EECN_ERR => {
                        "IBV_WC_INV_EECN_ERR: Invalid EE Context Number"
                    }
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
                    "RDMA read failed: {} (status: {:?}, vendor_err: {:?})",
                    status_str,
                    status,
                    err
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ib_socket() {
        let socket = IBSocket::new();
        assert!(socket.is_ok());
    }
}
