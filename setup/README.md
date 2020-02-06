In here go scripts that:
- set up the VPC, using either a provisioning tool (e.g. Terraform) or aws-cli, intelligently use imaging to minimize redundant setup
    - in each EC2 instance:
        - set up the machine
            - set hostname
            - set timezone
            - add repos
                - AdoptOpenJDK
        - install the necessary software
        - `git clone` this repo and create sym links to ../config/ files as appropriate

---
Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
