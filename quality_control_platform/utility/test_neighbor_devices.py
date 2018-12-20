from neighbor_devices import NeighborDevices
import time


def test_find_nearest_qc_devices(nei_dev):
    res = nei_dev.find_nearest_qc_devices('YSRDPM250000007974', 2, 'PM25', 2, True)
    print(res)


def test_find_nearest_devices(nei_dev):
    res = nei_dev.find_nearest_devices('YSRDPM250000004796', 'PM25', 2)
    print(res)


def test_find_nearest_site(nei_dev):
    res = nei_dev.find_nearest_site('YSRDPM250000004796',4)
    print(res)


if __name__ == "__main__":
    t1=time.time()
    nei_dev = NeighborDevices(city_id=[1])
    test_find_nearest_qc_devices(nei_dev)
    test_find_nearest_devices(nei_dev)
    test_find_nearest_site(nei_dev)
    t2=time.time()
    print(t2-t1)



