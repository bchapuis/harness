# The container mechanism's runner image (`machine_host::container`, machine spec
# §2.1): the scaffolding that turns a container into a machine's guest. It holds
# no user-visible state — the user's rootfs is the disk-facet image `init.sh`
# loop-mounts and chroots into.
#
# util-linux for losetup and mount; socat because the host reaches the guest
# agent's unix socket through `docker exec` (see init.sh). The image must be the
# same architecture as the rootfs the machine was provisioned from: the chroot
# runs the guest's own binaries under this kernel.
FROM alpine:3.20
RUN apk add --no-cache util-linux socat
COPY init.sh /init.sh
# Set here, not assumed: a copy of this directory that lost the mode bit would
# otherwise fail at container start rather than at build.
RUN chmod 0755 /init.sh
ENTRYPOINT ["/init.sh"]
