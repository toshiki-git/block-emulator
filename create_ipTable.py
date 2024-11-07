import json

def generate_shard_ports(num_shards=65, base_port=32000):
    shard_ports = {}

    for shard in range(num_shards):
        shard_ports[str(shard)] = {
            str(i): f"127.0.0.1:{base_port + shard * 10 + i}"
            for i in range(4)
        }
    
    return shard_ports

# JSON形式で出力
shard_ports = generate_shard_ports()
print(json.dumps(shard_ports, indent=2))
