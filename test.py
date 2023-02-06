from setuptools import find_packages

print(find_packages(include=["th2","th2.data_services","th2.data_services.lwdp","th2.data_services.lwdp.*"]))