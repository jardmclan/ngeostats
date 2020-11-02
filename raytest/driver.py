import ray
import os

# cores = 16

#need cluster address?
ray.init(address = os.environ["ip_head"])

print(ray.nodes())

@ray.remote
def node_exec(i):
    #test node output
    print(ray.services.get_node_ip_address(), i)




for i in range(1000):
    node_exec.remote(i)


#can use actors to share a single ftp connection pool, note that actors will be similar to managed classes in that they will have their own process
#note it looks like you can use a ray future as an input to an f.remote call and it will use the result of the ray future

