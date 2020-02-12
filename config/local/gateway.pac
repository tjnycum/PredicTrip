// Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.
function FindProxyForURL(url, host)
{
	if (isPlainHostName(host) || isInNet(dnsResolve(host), "10.0.0.0", "255.255.255.240")) {
        // if host is within CIDR block of the VPC subnet we've set up, reach it through SSH tunnel to gateway's HTTP proxy server
        return "PROXY localhost:8888";
    } else {
        return "DIRECT";
    }
}