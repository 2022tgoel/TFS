# TFS: A high-performance networked filesystem

For a description of the design, see [the paper](./paper.pdf) or the [presentation](https://docs.google.com/presentation/d/1fSLjpgp9EG-cxb1CdYCkz4d2E1V-QAWIQkQRKWq3bNU/edit?usp=sharing).

This filesystem is built to run on the MIT engaging cluster.

## Download Dependencies

* Rust:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* FUSE (Filesystem in Userspace):

fuse kernel module must be loaded (`lsmod | grep fuse`)

On Rocky Linux 8.10:
```
sudo dnf install fuse
```
See `fuse/` for the user installation for fuse2 libraries and headers that I did on MIT Engaging.
You can add this to your `.bashrc`:

```
export PKG_CONFIG_PATH=~/.local/lib64/pkgconfig/
export LIBRARY_PATH=~/.local/lib64/:$LIBRARY_PATH
export LD_LIBRARY_PATH=~/.local/lib64/:$LD_LIBRARY_PATH
```

* Python

* Zookeeper

* CMake/Make (for ibverbs rust crate)