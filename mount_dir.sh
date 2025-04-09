mkdir -p fuse_test
# Trap SIGINT and SIGTERM
trap "fusermount -u fuse_test" SIGINT SIGTERM
# Run the mount command
RUST_LOG=info ./target/debug/mount fuse_test $1 $2

