#! /bin/bash
file `find -not -size +1M` | grep text | less | cut -f 1 -d : |xargs ls -rt | xargs grep TIME | grep -v TOTAL
