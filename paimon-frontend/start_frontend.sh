#!/usr/bin/env bash
set -e
PORT=$(grep 'AUTO_PORT0' /home/web_server/PORT_INFO.json | cut -c 14-18)
RESOLVER=$(awk '/^nameserver/{printf "%s ", $2}' /etc/resolv.conf)
RESOLVER=${RESOLVER:- 8.8.8.8}
sed -e "s/PORT/${PORT}/g" -e "s/RESOLVER/${RESOLVER}/g" /nginx.conf.template > /etc/nginx/conf.d/default.conf
sed -i 's/worker_processes  .*/worker_processes  1;/' /data/nginx/conf/nginx.conf
/data/nginx/sbin/nginx -g 'daemon off;'