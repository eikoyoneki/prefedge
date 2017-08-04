#! /bin/bash

CACHE=$3
echo $((128*(2**30))) > /tmp/cgroup/0/memory.limit_in_bytes

if [ $CACHE -eq -1 ]; then
	echo Preloading...
	for i in sdc sdd sde sdf md127; do 
		echo 128 > /sys/block/$i/queue/read_ahead_kb; 
	done

	dd if=$5.vertices of=/dev/null
	dd if=$5.calist of=/dev/null
	dd if=$5.rew of=/dev/null
	dd if=$5.rew_index of=/dev/null

else

	CACHE=$((CACHE*(2**20)))


	BALLOON_CHK=1 BALLOON_TGT=0 $4 $5 $6 $7 2>&1 2>&1 | grep mlock | grep -o [0-9]\* > /tmp/mlock_size

	MLOCK=`cat /tmp/mlock_size`

	echo $3 >&2
	echo $((CACHE+MLOCK)) > /tmp/cgroup/0/memory.limit_in_bytes

	sysctl vm.drop_caches=3
fi

for i in /sys/block/sd?; do
        echo 0 > $i/queue/read_ahead_kb;
done

if echo $4 | grep sssp-trunc; then
	#ASSUME_UNDIRECTED=1 BALLOON_TGT=$(((32-CACHE)*(2**30))) RAND_IFT=$1 SEQ_IFT=$2 $4 $5 $6 $7;
	ASSUME_UNDIRECTED=1 BALLOON_TGT=0 RAND_IFT=$1 SEQ_IFT=$2 $4 $5 $6 $7;

else
	#BALLOON_TGT=$(((32-CACHE)*(2**30))) RAND_IFT=$1 SEQ_IFT=$2 $4 $5 $6 $7;
	OMP_NUM_THREADS=$1 ALAF=1 BALLOON_TGT=0 RAND_IFT=$1 SEQ_IFT=$2 $4 $5 $6 $7;

fi


#BALLOON_TGT=$(((TGT-(2*(2**20)))*1024)) RAND_IFT=$1 SEQ_IFT=$2 $3 $4 $5 $6 $7
