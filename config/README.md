For configuration items independent of code implementation, including the various config files of software packages used
in the EC2 instances:
- hadoop-site.xml
- hdfs-site.xml
- hbase-site.xml
- spark-env.sh
- etc.

Include in them all (currently) default settings of those package that we rely upon, to maximize forward compatibility.
(In case package defaults change in the future.)

Notes regarding setup:
- Hadoop version we're using expects the `workers` file to be named `slaves`, so it'll need to be either renamed or 
symlinked as such.
- Be mindful that bash itself will _ignore_ any `~/.profile` if a `~/.bash_profile` exists. Therefore, consider making 
your `.bash_profile` itself look for and source any `~/.profile`. Reason for putting things in .profile rather than 
.bash_profile: abstraction. If it's script that would work (and be useful) in any POSIX-compliant shell, it should
probably be put where any such shell would find and run it.
- User is responsible for taking steps (e.g. creating /etc/hosts entries) to enable hosts to resolve the host names used
in the configuration files.

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
