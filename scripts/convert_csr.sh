#! /bin/bash
CODE_PATH=/local/scratch/kn290/repo2/hpgp
SSD_PATH=/local/scratch-4
cat $1 | perl -f $CODE_PATH/scripts/graphgen_to_ir.pl > $1.ir
$CODE_PATH/scripts/sort_ir.sh $CODE_PATH/../../sort $1.ir $PWD
export LD_LIBRARY_PATH=$CODE_PATH/code:$LD_LIBRARY_PATH
$CODE_PATH/code/graph500_to_csr $1.ir.eout $1.ir.ein $1.csr
#cp $1.uncomp.* $SSD_PATH
pushd $SSD_PATH
#$CODE_PATH/code/static_optimisation $1.uncomp $1.comp
#$CODE_PATH/code/gen_random_edge_weights_csr $1.csr
popd

