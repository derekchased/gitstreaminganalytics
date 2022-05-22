# http://docs.openstack.org/developer/python-novaclient/ref/v2/servers.html
import time, os, sys, random, re
import inspect
from os import environ as env

from  novaclient import client
import keystoneclient.v3.client as ksclient
from keystoneauth1 import loading
from keystoneauth1 import session


#flavor = ["ssc.medium","ssc.medium", "ssc.medium"]
flavor = "ssc.medium"
private_net = "UPPMAX 2022/1-1 Internal IPv4 Network"
floating_ip_pool_name = None
floating_ip = None
image_name = "Ubuntu 20.04 - 2021.03.23"
ssh_key ="ssh_key_snic_cloud_east"

loader = loading.get_plugin_loader('password')

auth = loader.load_from_options(auth_url=env['OS_AUTH_URL'],
                                username=env['OS_USERNAME'],
                                password=env['OS_PASSWORD'],
                                project_name=env['OS_PROJECT_NAME'],
                                project_domain_id=env['OS_PROJECT_DOMAIN_ID'],
                                #project_id=env['OS_PROJECT_ID'],
                                user_domain_name=env['OS_USER_DOMAIN_NAME'])

sess = session.Session(auth=auth)
nova = client.Client('2.1', session=sess)
print ("user authorization completed.")

image = nova.glance.find_image(image_name)

#flavors = [nova.flavors.find(name=f) for f in flavor]
flavor = nova.flavors.find(name=flavor)

if private_net != None:
    net = nova.neutron.find_network(private_net)
    nics = [{'net-id': net.id}]
else:
    sys.exit("private-net not defined.")

#print("Path at terminal when executing this file")
#print(os.getcwd() + "\n")
cfg_file_path =  os.getcwd()+'/cloud-cfg.txt'
if os.path.isfile(cfg_file_path):
    userdata = open(cfg_file_path)
else:
    sys.exit("cloud-cfg.txt is not in current working directory")


secgroups = ['default']

print ("Creating instances ... ")
instance_a = nova.servers.create(name="G14-dev1", image=image, flavor=flavor, key_name=ssh_key,userdata=userdata, nics=nics,security_groups=secgroups)
instance_b = nova.servers.create(name="G14-dev2", image=image, flavor=flavor, key_name=ssh_key,userdata=userdata, nics=nics,security_groups=secgroups)
instance_c = nova.servers.create(name="G14-dev3", image=image, flavor=flavor, key_name=ssh_key,userdata=userdata, nics=nics,security_groups=secgroups)
inst_status_b = instance_b.status
inst_status_c = instance_c.status
inst_status_a = instance_a.status


print ("waiting for 10 seconds.. ")
time.sleep(10)

while inst_status_a == 'BUILD' or inst_status_b == 'BUILD' or inst_status_c == 'BUILD':
    print ("Instance: "+instance_a.name+" is in "+inst_status_a+" state, sleeping for 5 seconds more...")
    print ("Instance: "+instance_b.name+" is in "+inst_status_b+" state, sleeping for 5 seconds more...")
    print ("Instance: "+instance_c.name+" is in "+inst_status_c+" state, sleeping for 5 seconds more...")
    time.sleep(5)
    instance_a = nova.servers.get(instance_a.id)
    inst_status_a = instance_a.status
    instance_b = nova.servers.get(instance_b.id)
    inst_status_b = instance_b.status
    instance_c = nova.servers.get(instance_c.id)
    inst_status_c = instance_c.status

ip_address_a = None
for network in instance_a.networks[private_net]:
    if re.match('\d+\.\d+\.\d+\.\d+', network):
        ip_address_a = network
        break
if ip_address_a is None:
    raise RuntimeError('No IP address assigned!')

ip_address_b = None
for network in instance_b.networks[private_net]:
    if re.match('\d+\.\d+\.\d+\.\d+', network):
        ip_address_b = network
        break
if ip_address_b is None:
    raise RuntimeError('No IP address assigned!')

ip_address_c = None
for network in instance_c.networks[private_net]:
    if re.match('\d+\.\d+\.\d+\.\d+', network):
        ip_address_c = network
        break
if ip_address_c is None:
    raise RuntimeError('No IP address assigned!')

#Allocate floating IP
#floating_ip_a = nova.floating_ips.create(nova.floating_ip_pools.list()[0].name)
#instance_a.add_floating_ip(floating_ip_a)
#floating_ip_b = nova.floating_ips.create(nova.floating_ip_pools.list()[0].name)
#instance_b.add_floating_ip(floating_ip_b)
#floating_ip_c = nova.floating_ips.create(nova.floating_ip_pools.list()[0].name)
#instance_c.add_floating_ip(floating_ip_c)

print ("Instance: "+ instance_b.name +" is in " + inst_status_b + " state" + " ip address: "+ ip_address_a)
print ("Instance: "+ instance_c.name +" is in " + inst_status_c + " state" + " ip address: "+ ip_address_b)
print ("Instance: "+ instance_c.name +" is in " + inst_status_c + " state" + " ip address: "+ ip_address_c)
