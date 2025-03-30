wget ftp://ftp.icm.edu.pl/vol/rzm7/linux-centos-vault/8.0.1905/BaseOS/x86_64/kickstart/Packages/fuse-devel-2.9.7-12.el8.x86_64.rpm 
rpm2cpio fuse-devel-2.9.7-12.el8.x86_64.rpm  | cpio -idmv

cp -r ./usr/include/fuse ~/.local/include/fuse
cp -r ./usr/lib64/pkgconfig ~/.local/lib64/pkgconfig
mv ./usr/lib64/libfuse.so ~/.local/lib64/
rm -r usr

mkdir -p ~/.local/lib64
wget ftp://ftp.icm.edu.pl/vol/rzm7/linux-centos-vault/8.0.1905/BaseOS/x86_64/kickstart/Packages/fuse-libs-2.9.7-12.el8.x86_64.rpm
rpm2cpio fuse-libs-2.9.7-12.el8.x86_64.rpm | cpio -idmv

mv ./usr/lib64/libfuse* ~/.local/lib64/
rm -r usr


