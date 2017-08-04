#! /bin/bash
swapoff -a
#GRAPH_BASEDIR=$1
#GRAPHS="graph-tw-vgraph-er-v20m-k100.txt.comp graph-g5-v128m-k50.txt.comp"
#graph-ws-v20m-k100.txt.comp graph-tw-v50m-k40.txt.comp"
#ALGS="bfs bfs_prefetch dfs conductance conductance_prefetch mis mis_prefetch scc wcc wcc_prefetch pagerank pagerank_prefetch sssp sssp_prefetch_aux"
#ALGS="bfs_prefetch pagerank_prefetch conductance_prefetch mis_prefetch sssp_prefetch_aux"
#ALGS="bfs_csr pagerank_csr sssp_csr"
#ALGS="pagerank_csr_prefetch pagerank_csr"
#ALGS="sssp_csr_opti_prefetch_aux bfs_csr_opti_prefetch"
#ALGS="sssp-trunc_prefetch_aux"
#ALGS="a-star-vertex a-star-vertex_prefetch_aux a-star-vertex_prefetch_aux a-star-vertex_prefetch_aux"
#ALGS="a-star-vertex_prefetch_aux"
#ALGS="a-star-vertex_prefetch"
#ALGS="sssp_csr_prefetch_aux"
#ALGS="bfs_csr_mincore"
#ALGS="bfs_csr_opti_prefetch"
#ALGS="bfs_csr_time_prefetch"
#ALGS="sssp_csr sssp_csr_opti_prefetch_aux pagerank_csr pagerank_csr_prefetch"
#ALGS="ssp2"
#ALGS="bfs_csr_mt"
#ALGS="bfs_csr"
ALGS=`cat algorithms.txt`

declare -A PARMS
declare -A PARMS2
PARMS[conductance]=50
PARMS[conductance_prefetch]=50
PARMS[pagerank]=5
PARMS[pagerank_prefetch]=5
PARMS[pagerank_csr]=1
PARMS[pagerank_csr_prefetch]=1
PARMS[sssp-trunc]=$3
PARMS[sssp-trunc_prefetch_aux]=$3
PARMS[a-star-vertex_prefetch_aux]=0
PARMS2[a-star-vertex_prefetch_aux]=979
PARMS[a-star-vertex]=0
PARMS2[a-star-vertex]=979

trap 'kill -9 `ps -C sar -o pid=` 2> /dev/null;
kill -9 `ps -C iotop -o pid=` 2> /dev/null;
kill `ps -C stap -o pid=` 2> /dev/null' 0



#for i in $GRAPHS; do
	for j in $ALGS; do
		#iostat -xdt /dev/sdd /dev/sde 1 > $2-files/$2-$j-iostatpre.txt &
		#vmstat 1 > $2-files/$2-$j-vmstatpre.txt &
		#./vfsstat.sh $j > $2-files/$2-$j-vfsstatpre.txt &
		sysctl vm.drop_caches=3
		rm -f $2-files/$2-$j-r$3-s$4-c$5-sarstats.dat
		sar -bBr -o $2-files/$2-$j-r$3-s$4-c$5-sarstats.dat 1 > /dev/null 2>&1 &
		iotop -bkPqqq -d 1 | grep "% $j" | grep -v grep > $2-files/$2-$j-r$3-s$4-c$5-iotop.txt & 
#		cachehit.sh > $2-files/$2-$j-r$3-s$4-c$5-cachehit.txt &
		echo $j $1 $2 $3 $4 $5;
		benchmark.sh $3 $4 $5 $j $1 ${PARMS[$j]:-0} ${PARMS2[$j]:-};
		kill -9 `ps -C sar -o pid=`
		kill -9 `ps -C iotop -o pid=`
		#kill -9 `ps -C vmstat -o pid=`
		#kill -9 `ps -C vfsstat.sh -o pid=`
		kill `ps -C stap -o pid=`
	done
#done
