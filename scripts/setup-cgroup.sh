#! /bin/bash
D=/tmp/cgroup

if [[ $1 == "" ]]; then

	echo Usage: setup-cgroup.sh \$\$
	exit
fi

rmdir $D/0
umount $D
rmdir $D
mkdir $D

mount -t cgroup -omemory xxx $D
mkdir $D/0
echo $1 > $D/0/tasks
