#! /bin/bash
for j in `find | grep 'csr.*sarstats.dat$'`; do for i in b B r u; do sadf -d $j -- -$i > $i.txt; done; paste b.txt B.txt r.txt u.txt | sed 's/\t/;/g' > $j.txt; done
cp `find | grep 'csr.*sarstats.dat.txt'` ../kn290/repoRASP/results/tracedata/`hostname -s`/
pushd ../kn290/repoRASP/results/tracedata/`hostname -s`/
svn add *
svn commit
popd

