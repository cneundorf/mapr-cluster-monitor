#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Cluster Monitor:
# by Christian Neundorf
Usage:
{f}
"""

import requests
import json
import urllib3
from time import strftime

def fetch(user, pwd, hostname):
	#r = requests.get('https://<mapr-webserver>:8443/rest/node/list?', auth=('user', 'password'), verify=False)
	r = requests.get('https://{hostname}:8443/rest/node/list?&filter=[csvc==fileserver]'.format(hostname=hostname), auth=(user, pwd), verify=False)
	#r = requests.get('https://localhost:8443/rest/node/list?&filter=[csvc==fileserver]', auth=('mapr', 'password'), verify=False)
	data = json.loads(r.text)
	#print json.dumps(data)
	data = data['data']
	#print json.dumps(data[0])
	return data


def run(data,hostname):
	"""
	Run comment
	"""
	current_time = (strftime("%Y-%m-%d %H:%M:%S"))
	file_time = (strftime("%Y-%m-%d"))
	total_rpcs=0
	total_diskioread=0
	total_diskiowrite=0
	total_diskmbread=0
	total_diskmbwrite=0
	total_net=0
	total_mapr=0
	total_put=0
	total_get=0
	total_scan=0
	total_disks=0
	total_fdisks=0
	total_davail=0
	total_dused=0
	total_dtotal=0
	total_cpus=0
	total_ttmapSlots=0
	total_ttmapUsed=0
	total_ttmapUsed=0
	total_ttReduceSlots=0
	total_ttReduceUsed=0
	total_mtotal=0
	total_mused=0

	from contextlib import nested
	with nested( open('logs/mapr_{}_node_metrics_{}.csv'.format(hostname,file_time), 'a+'), open('logs/mapr_{}_cluster_metrics_{}.csv'.format(hostname,file_time), 'a+') ) as (node_metrics, cluster_metrics):
		print "╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗"
		print "║                                                     MapR Node Performance                                                         ║"
		print "╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣ "
		print "║{:^14}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║".format('hostname','cpu','rpc', 'drio', 'dwio', 'drmb', 'dwmb','net_in','net_out','mapr_in','mapr_out','put','get','scan')
		for item in data:
			nodename = item['hostname']
			rpcs = int(item['rpcs'])
			dreads = int(item['dreads'])
			dwrites = int(item['dwrites'])
			dreadK = int(item['dreadK'])
			dreadK = dreadK /1000
			dwriteK = int(item['dwriteK'])
			dwriteK = dwriteK /1000
			put_s = int(item['numPutsInLastTenSeconds']/10)
			get_s = int(item['numGetsInLastTenSeconds']/10)
			scan_s = int(item['numScansInLastTenSeconds']/10)
			net_in = int(item['bytesReceived'])/1000
			net_out = int(item['bytesSent'])/1000
			mapr_rpc_in = int(item['rpcin'])/1000/1000
			mapr_rpc_out = int(item['rpcout'])/1000/1000
			cpu = int(item['utilization'])
			total_rpcs = rpcs + total_rpcs
			total_diskioread = dreads + total_diskioread
			total_diskiowrite = dwrites + total_diskiowrite
			total_diskmbread = dreadK + total_diskmbread
			total_diskmbwrite = dwriteK + total_diskmbwrite
			total_iops = total_diskiowrite + total_diskioread
			total_net = net_in + net_out + total_net
			total_mapr = mapr_rpc_in + mapr_rpc_out + total_mapr
			total_put = put_s + total_put
			total_get = get_s + total_get
			total_scan = scan_s + total_scan
			total_db = total_put + total_get + total_scan
			disks = int(item['MapRfs disks'])
			fdisks = int(item['faileddisks'])
			davail = int(item['davail'])
			dused = int(item['dused'])
			dtotal = int(item['dtotal'])
			cpus = int(item['cpus'])
			ttmapSlots = int(item['ttmapSlots'])
			ttmapUsed = int(item['ttmapUsed'])
			ttmapUsed = int(item['ttmapUsed'])
			ttReduceSlots = int(item['ttReduceSlots'])
			ttReduceUsed = int(item['ttReduceUsed'])
			mtotal = int(item['mtotal'])
			mused = int(item['mused'])
			total_disks = disks + total_disks
			total_fdisks = fdisks + total_fdisks
			total_davail = davail + total_davail
			total_dused = dused + total_dused
			total_dtotal = dtotal + total_dtotal
			total_cpus = cpus + total_cpus
			total_ttmapSlots = ttmapSlots + total_ttmapSlots
			total_ttmapUsed = ttmapUsed + total_ttmapUsed
			total_ttReduceSlots = ttReduceSlots + total_ttReduceSlots
			total_ttReduceUsed = ttReduceUsed + total_ttReduceUsed
			total_mtotal = mtotal + total_mtotal
			total_mused = mused + total_mused


			line = "║{:^14}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║{:^8}║".format(nodename,cpu, rpcs, dreads, dwrites, dreadK, dwriteK, net_in, net_out, mapr_rpc_in, mapr_rpc_out, put_s, get_s, scan_s)
			print line
			print "╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝"
			node_metrics_collect = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(current_time,nodename,cpu, rpcs, dreads, dwrites, dreadK, dwriteK, net_in, net_out,mapr_rpc_in, mapr_rpc_out, put_s, get_s, scan_s, disks,fdisks,davail,dused,dtotal,cpus,ttmapSlots,ttmapUsed,ttmapUsed,ttReduceSlots,ttReduceUsed,mtotal,mused)
			node_metrics.write(node_metrics_collect+'\n')

		print ""
		print ""
		print ""
		print "╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗"
		print "║                                                      MapR Cluster Performance                                                     ║"
		print "╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣ "
		print "║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^19}║".format('total','total','total', 'total', 'total', 'total','total', 'total', 'total')
		print "║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^19}║".format('rpc/s','disk_iops','disk_r_iops', 'disk_w_iops', 'disk_r_mb/s', 'disk_w_mb/s', 'net mb/s', 'mapr mb/s')
		cluster = "║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^15}║{:^19}║".format(total_rpcs, total_iops, total_diskioread, total_diskiowrite, total_diskmbread, total_diskmbwrite,total_net,total_mapr)
		print cluster
		print "╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣"
		print "║{:^15}║{:^15}║{:^15}║{:^15}║{:^21}║{:^22}║{:^22}║".format('total','total','total', 'total', 'balancer', 'rereplication', 'rolebalancer')
		print "║{:^15}║{:^15}║{:^15}║{:^15}║{:^21}║{:^22}║{:^22}║".format('db ops','db put','db get', 'db_scan', 'status', 'status', 'status')
		cluster2 = "║{:^15}║{:^15}║{:^15}║{:^15}║{:^21}║{:^22}║{:^22}║".format(total_db, total_put, total_get, total_scan,"N/A" ,"N/A" ,"N/A")
		print cluster2
		print "╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝"

		cluster_metrics_collect = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(current_time,total_db, total_put, total_get, total_scan,total_rpcs, total_iops, total_diskioread, total_diskiowrite, total_diskmbread, total_diskmbwrite,total_disks,total_fdisks,total_dtotal,total_dused,total_davail,total_net,total_mapr,total_cpus,total_ttmapSlots,ttmapUsed,total_ttmapUsed,total_ttReduceSlots,total_ttReduceUsed,total_mtotal,total_mused)
		cluster_metrics.write(cluster_metrics_collect+'\n')





if __name__ == '__main__':
	import argparse
	argp = argparse.ArgumentParser(__doc__.format(f=__file__))
	argp.add_argument('-u', '--user', help=run.__doc__)
	argp.add_argument('-p', '--password', help=run.__doc__)
	argp.add_argument('-m', '--machine', help=run.__doc__)
	args = argp.parse_args()
	if args.user and args.password and args.machine:
		data = fetch(args.user, args.password, args.machine)
		run( data, args.machine )
	else:
		argp.print_help()
