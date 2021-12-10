import random


def pick_device():
    device_list = ['Server-A', 'Server-B', 'Server-C', 'Server-D', 'Server-E', 'Server-F']
    print(random.choice(device_list))


pick_device()
