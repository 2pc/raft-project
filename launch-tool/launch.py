#!/usr/bin/env python3
from kubernetes import client, config

import argparse
import copy
import os
import subprocess
import sys
import yaml

def peer_name_to_group_peer(peer_name):
    tmp = peer_name.split('-')
    return tmp[0][4:], tmp[1].split(':')[0]

def find_pods(v1):
    """Find pods started by us or at least running shardkv-peer/shardmaster-peer"""
    ret = v1.list_pod_for_all_namespaces(watch=False)
    def pod_filter(p):
        return p.metadata.namespace == "default" and \
                len(p.spec.containers) == 1 and \
                (p.spec.containers[0].image == 'local/shardkv-peer' or p.spec.containers[0].image == 'local/shardmaster-peer')
    pods_we_own = filter(pod_filter, ret.items)
    return pods_we_own

def shutdown_pod(v1, name, namespace):
    """Shutdown a single pod"""
    response = v1.delete_namespaced_pod(name, \
            namespace,\
            client.V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy='Foreground')
    response = v1.delete_namespaced_service(name, \
            namespace,\
            client.V1DeleteOptions(),
            grace_period_seconds=0,
            propagation_policy='Foreground')

def shutdown_pods(v1, pods):
    """Shutdown the given set of pods"""
    for i in pods:
        try:
            shutdown_pod(v1, i.metadata.name, i.metadata.namespace)
        except Exception as e:
            print("Error in killing %s %s"%(i, e), file=sys.stderr)

def get_service(v1, service):
    """Get service spec for service"""
    return v1.list_service_for_all_namespaces(watch=False, field_selector="metadata.name=%s"%service)

def boot_pod(v1, pod_spec, service_spec, name):
    """Boot a single pod"""
    pod_spec = copy.deepcopy(pod_spec)
    # Create a pod spec for this pod.
    pod_spec['metadata']['name'] = name
    pod_spec['metadata']['labels']['app'] = name
    pod_spec['spec']['containers'][0]['ports'][0]['name']="%s-client"%name
    pod_spec['spec']['containers'][0]['ports'][1]['name']="%s-raft"%name

    service_spec = copy.deepcopy(service_spec)
    # Create a service spec for this service
    service_spec['metadata']['name'] = name
    service_spec['spec']['selector']['app'] = name
    service_spec['spec']['ports'][0]['targetPort'] = "%s-client"%name
    service_spec['spec']['ports'][1]['targetPort'] = "%s-raft"%name
    try:
        response = v1.create_namespaced_pod('default', pod_spec)
        response = v1.create_namespaced_service('default', service_spec)
    except:
        print("Could not launch pod or service")
        raise

def init():
    """Initialize and get client"""
    config.load_kube_config()
    v1 = client.CoreV1Api()
    return v1

def shutdown(args):
    """Shutdown pods run by us. This might take a while after returning"""
    v1 = init()
    pods = find_pods(v1)
    shutdown_pods(v1, pods)

def show(args):
    """Show pods launched by us"""
    v1 = init()
    pods = find_pods(v1)
    for pod in pods:
        print("%s"%pod.metadata.name)

def boot(args):
    """Launch a set of pods"""
    v1 = init()
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))

        """ launch shardkv groups """
        shardkv_pod_spec = specs[0]
        shardkv_service_spec = specs[1]
        num_groups   = args.groups
        num_services = args.peers
        for group_id in range(1, num_groups + 1):
            for peer_id in range(num_services):
                peer_name = "peer%d-%d" % (group_id, peer_id)
                boot_pod(v1, shardkv_pod_spec, shardkv_service_spec, peer_name)

        """ launch shardmaster group """
        shardmaster_pod_spec = specs[2]
        shardmaster_service_spec = specs[3]
        num_services = args.peers
        for peer_id in range(num_services):
            peer_name = "peer0-%d" % (peer_id)
            boot_pod(v1, shardmaster_pod_spec, shardmaster_service_spec, peer_name)

def kill(args):
    """Kill selected peer"""
    v1 = init()
    pods = find_pods(v1)
    peer = args.peer
    pod = list(filter(lambda i: i.metadata.name == peer, pods))
    if len(pod) != 1:
        sys.exit(1)
    shutdown_pod(v1, pod[0].metadata.name, pod[0].metadata.namespace)

def launch(args):
    """Launch an individual peer"""
    v1 = init()
    pods = find_pods(v1)
    peers = list(map(lambda i: i.metadata.name, pods))
    pod = args.peer
    if pod in peers:
        print("%d is already running"%args.peer, out=sys.stderr)
        sys.exit(1)
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        group_id, _ = peer_name_to_group_peer(pod)
        if group_id == "0" :
            pod_spec = specs[2]
            service_spec = specs[3]
            boot_pod(v1, pod_spec, service_spec, pod)
        else :
            pod_spec = specs[0]
            service_spec = specs[1]
            boot_pod(v1, pod_spec, service_spec, pod)

def get_service_url(args):
    """Get service URL for peer"""
    v1 = init()
    ip = subprocess.run('minikube ip', check=True, stdout=subprocess.PIPE, shell=True).stdout\
            .decode('utf-8').strip()
    svcs = get_service(v1, args.peer)
    if len(svcs.items) != 1:
        print("Could not find service", file=sys.stderr)
        sys.exit(1)
    svc = svcs.items[0]
    ports = svc.spec.ports
    with open(os.path.join(sys.path[0], 'pod-template.yml')) as f:
        specs = list(yaml.load_all(f))
        service_spec = specs[1]
        for port in ports:
            if port.port == service_spec['spec']['ports'][0]['port']:
                print('%s:%d'%(ip, port.node_port))
                sys.exit(0)
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(prog=sys.argv[0])
    subparsers = parser.add_subparsers(help="sub-command help", dest='command')
    subparsers.required = True
    
    shutdown_parser = subparsers.add_parser("shutdown")
    shutdown_parser.set_defaults(func=shutdown)

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func = show)

    run_parser = subparsers.add_parser("boot")
    run_parser.add_argument('groups', type=int, default=0, help='How many shardgroups? (excluding shardmaster')
    run_parser.add_argument('peers', type=int, default=5, help='How many peers in each shardgroup?')
    run_parser.set_defaults(func = boot)

    kill_parser = subparsers.add_parser("kill")
    kill_parser.add_argument('peer', type=str, help='Which peer should die')
    kill_parser.set_defaults(func=kill)
    
    kill_parser = subparsers.add_parser("launch")
    kill_parser.add_argument('peer', type=str, help='Which peer should be launched')
    kill_parser.set_defaults(func=launch)

    svc_parser = subparsers.add_parser("client-url")
    svc_parser.add_argument('peer', type=str, help="Which peer do you need URL for")
    svc_parser.set_defaults(func=get_service_url)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
    sys.exit(0)
