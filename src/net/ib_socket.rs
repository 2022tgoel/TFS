use super::utils::my_name;
use anyhow::Result;
use ibverbs::{
    CompletionQueue, Context as IbContext, MemoryRegion, ProtectionDomain, QueuePair,
    QueuePairEndpoint, ibv_qp_type,
};
use serde_json;
use zeromq::{Socket, SocketRecv, SocketSend};

const SERVER_PORT: u16 = 5555;

/// Represents an InfiniBand socket for RDMA communication
pub struct IBSocket {
    context: &'static IbContext,
    pd: &'static ProtectionDomain<'static>,
    cq: &'static CompletionQueue<'static>,
    qp: Option<QueuePair<'static>>,
    mr: Option<MemoryRegion<u8>>,
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
            qp: None,
            mr: None,
        })
    }

    /// Registers memory region for RDMA operations
    pub fn register_memory(&mut self, buffer: &mut [u8]) -> Result<()> {
        Ok(())
    }

    /// Connects to a remote node
    pub async fn connect(&mut self, remote_addr: String) -> Result<()> {
        // Create the queue pair
        let prepared_qp = self
            .pd
            .create_qp(self.cq, self.cq, ibv_qp_type::IBV_QPT_RC)
            .set_gid_index(1)
            .build()?;

        // Send the endpoint information
        if my_name()? < remote_addr {
            let mut zmq_socket = zeromq::RouterSocket::new();
            let addr = format!("tcp://{}:{}", my_name()?, SERVER_PORT);
            zmq_socket.bind(&addr).await?;
            let resp = zmq_socket.recv().await?;

            let msg = serde_json::to_string(&prepared_qp.endpoint())?;
            let mut zmsg = zeromq::ZmqMessage::from(
                resp.get(0)
                    .ok_or(anyhow::anyhow!("No zmq message received"))?
                    .clone(),
            );
            zmsg.push_back(msg.into());
            zmq_socket.send(zmsg).await?;
            // Do the infiniband handshake
            let endpoint_str = String::from_utf8(
                resp.get(1)
                    .ok_or(anyhow::anyhow!("Invalid zmq message received"))?
                    .to_vec(),
            )?;
            let endpoint: QueuePairEndpoint = serde_json::from_str(&endpoint_str)?;
            let qp = prepared_qp.handshake(endpoint)?;
            // Store the queue pair
            self.qp = Some(qp);

            Ok(())
        } else {
            let mut zmq_socket = zeromq::DealerSocket::new();
            let addr = format!("tcp://{}:{}", remote_addr, SERVER_PORT);
            zmq_socket.connect(&addr).await?;
            let msg = serde_json::to_string(&prepared_qp.endpoint())?;
            zmq_socket.send(msg.into()).await?;
            // Do the infiniband handshake
            let resp = String::from_utf8(
                zmq_socket
                    .recv()
                    .await?
                    .get(0)
                    .ok_or(anyhow::anyhow!("Invalid zmq message received"))?
                    .to_vec(),
            )?;
            let endpoint: QueuePairEndpoint = serde_json::from_str(&resp)?;
            let qp = prepared_qp.handshake(endpoint)?;
            // Store the queue pair
            self.qp = Some(qp);

            Ok(())
        }
    }

    /// Sends data using RDMA
    pub fn send(&self, data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Receives data using RDMA
    pub fn receive(&self, buffer: &mut [u8]) -> Result<()> {
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
