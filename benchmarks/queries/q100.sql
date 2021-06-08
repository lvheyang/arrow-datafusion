select l_returnflag, l_linestatus, avg(l_discount) as avg_disc
from lineitem group by l_returnflag, l_linestatus
