For configuration items independent of code implementation, including the various config files of software packages used in the EC2 instances:
- hadoop-site.xml
- hdfs-site.xml
- hbase-site.xml
- spark-env.sh
- etc.

Include in them all (currently) default settings of those package that we rely upon, to maximize forward compatibility. (The packages' defaults might change in the future.)

What about `/etc/hosts`?

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
