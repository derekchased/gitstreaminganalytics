# http://docs.openstack.org/developer/python-novaclient/ref/v2/servers.html
import time, os, sys, random, re
import inspect
from os import environ as env

from  novaclient import client
import keystoneclient.v3.client as ksclient
from keystoneauth1 import loading
from keystoneauth1 import session
from pathlib import PurePath, Path
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-q", "--quantity", 
    help='Quantity of vms to launch',
    required=True)

parser.add_argument("-n", "--name", 
    help='Name of vm',
    required=True)

parser.add_argument("-f", "--flavor", 
    help='Size of flavor',
    choices=['s', 'm', 'l', 'h'],
    required=True)

parser.add_argument("-s", "--sshkey", 
    help='Name of SSH Key',
    required=True)

parser.add_argument("-u", "--userdata", 
    help='Userdata file name. File must be in the cloud-init folder',
    required=True)

args = parser.parse_args()

_trace = False
if _trace == True:
    for arg in vars(args):
        print(arg, getattr(args, arg))

def convert_flavor(f):
    if f == "s":
        return "ssc.small"
    elif f == "m":
        return "ssc.medium"
    elif f == "l":
        return "ssc.large"
    elif f == "h":
        return "ssc.large.highcpu"
    else:
        return f

# From args
num_vms = args.quantity
name_vm = args.name
flavor = convert_flavor(args.flavor)
ssh_key = args.sshkey
cfg_file = args.userdata

# Defaults
private_net = "UPPMAX 2022/1-1 Internal IPv4 Network"
floating_ip_pool_name = None
floating_ip = None
image_name = "Ubuntu 20.04 - 2021.03.23"

loader = loading.get_plugin_loader('password')

auth = loader.load_from_options(auth_url=env['OS_AUTH_URL'],
                                username=env['OS_USERNAME'],
                                password=env['OS_PASSWORD'],
                                project_name=env['OS_PROJECT_NAME'],
                                project_domain_id=env['OS_PROJECT_DOMAIN_ID'],
                                #project_id=env['OS_PROJECT_ID'],
                                user_domain_name=env['OS_USER_DOMAIN_NAME'])


# Establish connection
sess = session.Session(auth=auth)
nova = client.Client('2.1', session=sess)
# print ("user authorization completed.")

# Get from nova
image = nova.glance.find_image(image_name)
flavor = nova.flavors.find(name=flavor)

if private_net != None:
    net = nova.neutron.find_network(private_net)
    nics = [{'net-id': net.id}]
else:
    sys.exit("private-net not defined.")

secgroups = ['default', 'derek_c3_2']

print ("Creating instances ... ")
instances = []
instances_status = []

for i in range(int(num_vms)):
    cfg_file_path =  Path.cwd().parent / "cloud-init" / cfg_file
    if os.path.isfile(cfg_file_path):
        userdata = open(cfg_file_path)
    else:
        sys.exit(f"{cfg_file} is not in current working directory")

    instance = nova.servers.create(name=name_vm+"-"+str(i), image=image, flavor=flavor, key_name=ssh_key,userdata=userdata, nics=nics,security_groups=secgroups)
    instances.append(instance)
    instances_status.append(instance.status)
    pass
print ("waiting for 10 seconds.. ")
time.sleep(10)  


while(any( map( lambda status: status == "BUILD" ,instances_status))):
    for i in range(len(instances)):
        print(f"{instances[i].name} has status {instances_status[i]}")
        instances[i] = nova.servers.get(instances[i].id)
        instances_status[i] = instances[i].status
    print("sleeping 5 seconds...")
    time.sleep(5)

ips = []
for instance in instances:
    ip_address = None
    for network in instance.networks[private_net]:
        if re.match('\d+\.\d+\.\d+\.\d+', network):
            ip_address = network
            ips.append(ip_address)
            break
    if ip_address is None:
        raise RuntimeError(f"{instance.name} - No IP address assigned!")

for instance, status, ip in zip(instances, instances_status, ips):
    print (f"{instance.name} is in {status} with ip  address: "+ ip)