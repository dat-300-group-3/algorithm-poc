
from datasketches import frequent_strings_sketch, frequent_items_error_type
import dask.dataframe as dd
import numpy as np
import pandas as pd

CSV_FILE_PATH = './data/24h_no_isAttributed.csv'
#CSV_FILE_PATH = './data/12h_device_not_1_2.csv'

sketch_ip = frequent_strings_sketch(128)
sketch_ip_device = frequent_strings_sketch(128)
sketch_ip_device_os = frequent_strings_sketch(128)
sketch_ip_device_os_channel = frequent_strings_sketch(128)
sketch_ip_device_os_channel_app = frequent_strings_sketch(128)

# Helper function to process each partition of the dataframe
def process_partition(partition):
    
    # Iterate over each row and concatenate 'ip' and 'device' columns
    for idx, row in partition.iterrows():
        ip =        str(row['ip'])
        device =    str(row['device'])
        os =        str(row['os'])
        channel =   str(row['channel'])
        app =       str(row['app'])

        sketch_ip.update(ip)
        sketch_ip_device.update(ip + '-' + device)
        sketch_ip_device_os.update(ip + '-' + device + '-' + os)
        sketch_ip_device_os_channel.update(ip + '-' + device + '-' + os + '-' + channel)
        sketch_ip_device_os_channel_app.update(ip + '-' + device + '-' + os + '-' + channel + '-' + app)
        

# Read the CSV file using Dask
df = dd.read_csv(CSV_FILE_PATH, dtype={'attributed_time': 'object'})  # Adjust dtype if necessary

# Apply the function to process each partition
df.map_partitions(process_partition).compute()

print(CSV_FILE_PATH)

ip_result =         sketch_ip.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
device_result =     sketch_ip_device.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
os_result =         sketch_ip_device_os.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
channel_result =    sketch_ip_device_os_channel.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
app_result =        sketch_ip_device_os_channel_app.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)

print("IP")
print(sketch_ip)
for ip_tuple in ip_result[:1000]:  # Slice to get the first 1000 elements
    print("IP: " + ip_tuple[0] + ", Count: " + str(ip_tuple[1]))

print("IP-Device")
print(sketch_ip_device)
for device_tuple in device_result[:1000]:  # Slice to get the first 1000 elements
    print("IP-Device: ", device_tuple[0], ", Count: ", device_tuple[1])

print("IP-Device-OS")
print(sketch_ip_device_os)
for os_tuple in os_result[:1000]:  # Slice to get the first 1000 elements
    print("IP-Device-OS: ", os_tuple[0], ", Count: ", os_tuple[1])

print("IP-Device-OS-Channel")
print(sketch_ip_device_os_channel)
for channel_tuple in channel_result[:1000]:  # Slice to get the first 1000 elements
    print("IP-Device-OS-Channel: ", channel_tuple[0], ", Count: ", channel_tuple[1])

print("IP-Device-OS-Channel-App")
print(sketch_ip_device_os_channel_app)
for app_tuple in app_result[:1000]:  # Slice to get the first 1000 elements
    print("IP-Device-OS-Channel-App: ", app_tuple[0], ", Count: ", app_tuple[1])

    
    

