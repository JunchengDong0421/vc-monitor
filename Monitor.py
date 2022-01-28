import asyncio
import atexit
import os
import warnings
from datetime import timedelta, datetime

import pandas as pd
from pyVim.connect import SmartConnect, SmartConnectNoSSL, Disconnect
from pyVmomi import vim

from Tree import DCTree, Node


class Monitor:
    def __init__(self, *args, **kwargs):
        """Define __init__ function.

        :param args: positional arguments for connect function
        :param kwargs: keyword arguments for connect function
        """
        self.args, self.kwargs = args, kwargs
        self.service_instance = None
        self.service_content = None
        # views
        self.datacenter_view = []
        self.datastore_view = []
        self.host_view = []
        self.vm_view = []
        self.cr_view = []
        # node structure
        self.trees = []
        self.nodes = []
        self.id_map = {}
        self.name_map = {}
        # performance management
        self.perf_manager = None
        self.all_counters = {}
        self.historical_intervals = []
        # provider information
        self.current_supported = None
        self.summary_supported = None
        self.refresh_rate = -1

    def connect(self, host, user, pwd, *args, verify=False, **kwargs):
        """Connect to vCenter Server and obtain service instance.

        :param host: hostname
        :param user: username
        :param pwd: password
        :param args: positional arguments for SmartConnect or SmartConnectNoSSL
        :param verify: if True then connect with SSL, else without SSL
        :param kwargs: keyword arguments for SmartConnect or SmartConnectNoSSL
        :rtype: NoneType
        """
        try:
            connect_func = SmartConnectNoSSL if not verify else SmartConnect
            self.service_instance = connect_func(*args, host=host, user=user, pwd=pwd, **kwargs)
            print(f"Connected to {host}")
        except Exception as e:
            print(f"Connection failed: {e}")

    def retrieve_content(self):
        """Retrieve content from service instance.

        :rtype: None
        """
        try:
            self.service_content = self.service_instance.RetrieveServiceContent()
            print("Service content retrieved")
        except Exception as e:
            print(f"Cannot retrieve content: {e}")

    def disconnect(self):
        """Disconnect from vCenter Server.

        :rtype: NoneType
        """
        try:
            Disconnect(self.service_instance)
            print("Disconnected to server")
        except Exception as e:
            print(f"Cannot disconnect to server: {e}")

    def update_views(self):
        """Update container views for datacenter, datastore, host, virtual machine, and compute resource.

        :rtype: NoneType
        """
        root_folder = self.service_content.rootFolder
        view_manager = self.service_content.viewManager
        self.datacenter_view = view_manager.CreateContainerView(root_folder, [vim.Datacenter], True).view
        self.datastore_view = view_manager.CreateContainerView(root_folder, [vim.Datastore], True).view
        self.host_view = view_manager.CreateContainerView(root_folder, [vim.HostSystem], True).view
        self.vm_view = view_manager.CreateContainerView(root_folder, [vim.VirtualMachine], True).view
        self.cr_view = view_manager.CreateContainerView(root_folder, [vim.ComputeResource], True).view

    @classmethod
    def get_child_objects(cls, folder_or_object, results=None):
        """Recursive method to obtain child managed objects from a root folder.

        :param folder_or_object: a root folder (vim.Folder) or a child object (vim.ManagedObject)
        :param results: result list to return in the end
        :rtype: list
        """
        if results is None:
            results = []
        if hasattr(folder_or_object, 'childEntity'):  # if folder
            for folder_or_object in folder_or_object.childEntity:
                cls.get_child_objects(folder_or_object, results)
        else:  # if object
            results.append(folder_or_object)
        return results

    @classmethod
    def _build_dc_tree(cls, dc):
        """Build a tree structure for vCenter Server with a datacenter object as root.

        :param dc: a datacenter object (vim.Datacenter)
        :rtype: DCTree
        """
        root = Node([], dc, [])  # a datacenter
        host_folder_list = cls.get_child_objects(dc.hostFolder)
        for h_cr in host_folder_list:
            root_child = Node([root], h_cr, [])  # a host or compute resource
            root.children.append(root_child)
            if isinstance(h_cr, vim.HostSystem):  # if host
                for vm in h_cr.vm:
                    host_child = Node([root_child], vm, [])  # a virtual machine
                    root_child.children.append(host_child)
            else:  # if compute resource (including cluster compute resource)
                for h in h_cr.host:
                    cr_child = Node([root_child], h, [])  # a host
                    root_child.children.append(cr_child)
                    for vm in h.vm:
                        host_child = Node([cr_child], vm, [])  # a virtual machine
                        cr_child.children.append(host_child)
        return DCTree(root)

    def build_struct(self):
        """Obtain tree structure, all nodes, node name and id maps of vCenter Server.

        :rtype: NoneType
        """
        self.trees = [self._build_dc_tree(dc) for dc in self.datacenter_view]
        self.nodes.extend(*(t.nodes for t in self.trees))
        self.id_map = {n.id: n.value.name for n in self.nodes}
        self.name_map = {v: k for k, v in self.id_map.items()}

    def manage_perf(self):
        """Obtain performance manager, all counters, and all historical intervals of vCenter Server.

        :rtype: NoneType
        """
        self.perf_manager = self.service_content.perfManager
        self.all_counters = {perf.key: perf for perf in self.perf_manager.perfCounter}
        # obtain historical intervals
        # vCenter Server Default Historical Intervals: (key, samplingPeriod, name, length, level)
        # 1, 300, "Past day", 86400, 1
        # 2, 1800, "Past week", 604800, 1
        # 3, 7200, "Past month“, 2592000, 1
        # 4, 86400, "Past year", 31536000, 1
        for i in self.perf_manager.historicalInterval:
            if i.enabled:
                self.historical_intervals.append(i.samplingPeriod)

    def query_provider(self):
        """Query for the provider information of server, including refresh rate and whether realtime stats supported.

        :rtype: NoneType
        """
        provider = vim.PerformanceManager.QueryPerfProviderSummary(self.perf_manager, self.vm_view[0])
        self.current_supported, self.summary_supported = provider.currentSupported, provider.summarySupported
        self.refresh_rate = provider.refreshRate  # usually 20 or -1

    def __enter__(self):
        """Define __enter__ function.

        :rtype: Monitor
        """
        self.connect(*self.args, **self.kwargs)
        self.retrieve_content()
        self.update_views()
        self.build_struct()
        self.manage_perf()
        self.query_provider()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Define __exit__ function.

        :param exc_type: exception (if any) type
        :param exc_val: exception (if any) value
        :param exc_tb: exception (if any) traceback
        :rtype: NoneType
        """
        self.disconnect()

    def init(self):
        """The method to call after instantiating Monitor out of a with clause.

        :rtype: NoneType
        """
        atexit.register(self.__exit__)
        self.__enter__()

    @staticmethod
    def _ensure_task(coro_or_func):  # will always return a task
        """Return a Task for given Coroutine or Callable (any functions).

        :param coro_or_func: a coroutine or a function
        :rtype: asyncio.Task
        """
        if asyncio.iscoroutine(coro_or_func):  # a coroutine
            coro = coro_or_func
        elif not callable(coro_or_func):
            raise TypeError(f"A task must either be a coroutine or a function, use functools.partial for passing "
                            f"arguments")
        elif asyncio.iscoroutinefunction(coro_or_func):  # a coroutine function
            coro = coro_or_func()
        else:  # a normal function
            async def _wrapper():  # wrap a synchronous function into an asynchronous one
                return coro_or_func()

            coro = _wrapper()
        return asyncio.create_task(coro)

    async def _main_loop(self, tasks):
        """Synchronize tasks and gather results. Perform the actual task executions.

        :param tasks: list of asyncio.Task objects
        :rtype: list
        """
        futures = [self._ensure_task(t) for t in tasks]
        results = await asyncio.gather(*futures)
        return results

    def main_loop(self, tasks):
        """Run given tasks in main event loop.

        :param tasks: list of coroutines or functions to run
        :rtype: list
        """
        results = asyncio.run(self._main_loop(tasks))
        return results

    def server_option_defaults(self):
        """Get the list of default server options.

        :rtype: list
        """
        opt_list = self.service_content.setting.supportedOption
        return [f"{o.key}" for o in opt_list]

    def _get_available_counters(self, entity):
        """Obtain available performance counters for given managed object.

        :param entity: a managed object
        :rtype: list
        """
        available_counters = vim.PerformanceManager.QueryAvailablePerfMetric(self.perf_manager, entity)
        return [c.counterId for c in available_counters]

    def _export_available_counters(self, entity=None, path=None):
        """Export available counters information of an entity to a csv file path.

        :param entity: managed object. None if exporting all available counters of server
        :param path: final csv path
        :rtype: NoneType
        """
        data = pd.DataFrame()
        counter_ids = self._get_available_counters(entity) if entity else self.all_counters.keys()
        for c in counter_ids:
            counter_info = self.all_counters[c]
            name_info = counter_info.nameInfo
            new_row = pd.DataFrame({'key': counter_info.key, 'label': name_info.label, 'summary': name_info.summary,
                                    'type': counter_info.rollupType}, index=[0])
            data = data.append(new_row, ignore_index=True)
        data = data.sort_values(by='key')
        if not os.path.exists('metrics'):
            os.mkdir('metrics')
        if not path:
            path = f"metrics{os.sep}{entity.name}.csv"
        data.to_csv(path, index=False)

    def export_vm_counters(self, path=None):
        """Export counters information of all virtual machines to a .csv file.

        :param path: output filepath
        :rtype: NoneType
        """
        for vm in self.vm_view:
            self._export_available_counters(vm, path)

    def export_host_counters(self, path=None):
        """Export counters information of all host systems to a .csv file.

        :param path: output filepath
        :rtype: NoneType
        """
        for h in self.host_view:
            self._export_available_counters(h, path)

    def export_all_counters(self, path=None):
        """Export all counters information of the server to a .csv file.

        :param path: output filepath
        :rtype: NoneType
        """
        self._export_available_counters(None, path)

    def _build_query(self, entity, counter_ids=None, instances=None, interval=None, start=None, end=None,
                     max_samples=None, fmt="normal"):
        """Build a query specification object (vim.PerformanceManager.QuerySpec) for given managed object.

        :param entity: a managed object
        :param counter_ids: keys of counters to query
        :param instances: which instances (e.g. network adapters, control wires) to query
        :param interval: interval of statistics to query
        :param start: start time of statistics to query
        :param end: end time of statistics to query
        :param max_samples: maximum number of samples to query
        :param fmt: format of query
        :rtype: vim.PerformanceManager.QuerySpec
        """
        if not (max_samples or start):
            raise TypeError("A time range or sample limit should be specified")
        if fmt not in ('csv', 'normal'):
            raise ValueError("Invalid format, can either be 'normal' or 'csv'")
        if self.current_supported:  # if the entity supports real-time (current) statistics
            if interval is None:
                interval = self.refresh_rate
            elif interval != self.refresh_rate and interval not in self.historical_intervals:
                raise ValueError(f"Invalid value '{interval}' for parameter 'interval', available ones are "
                                 f"{', '.join([str(i) for i in self.historical_intervals])}")
        elif self.summary_supported:  # if the entity only supports historical (aggregated) statistics, refreshRate = -1
            if interval not in self.historical_intervals:  # including an unspecified interval (interval = None)
                raise ValueError(f"Invalid value '{interval}' for parameter 'interval', available ones are "
                                 f"{', '.join([str(i) for i in self.historical_intervals])}")
        else:
            raise RuntimeError("Statistics unavailable for this entity!")
        if interval in self.historical_intervals and max_samples:  # max_samples is ignored for historical statistics
            warnings.warn("max_samples will not apply because interval specified is not real-time, use a time range "
                          "instead", RuntimeWarning)
        if counter_ids is None:
            counter_ids = []  # default to be no counters
        if instances is None:
            instances = ["*"]  # default to be all instances, note an empty string means summed statistics
        query = vim.PerformanceManager.QuerySpec()
        query.entity = entity
        query.metricId = [vim.PerformanceManager.MetricId(counterId=c, instance=i)
                          for c in counter_ids for i in instances]
        query.intervalId = interval  # must be one of the enabled intervals, unit: second(s)
        query.startTime = start  # the returned samples DO NOT include the sample at startTime
        query.endTime = end  # the returned samples include the sample at endTime
        query.maxSample = max_samples
        query.format = fmt
        return query

    def realtime_stats(self, entity, counter_ids=None, instances=None):
        """Obtain realtime statistics of given managed object.

        :param entity: a managed object
        :param counter_ids: keys of counters to query
        :param instances: which instances (e.g. network adapters, control wires) to query
        :rtype: vim.PerformanceManager.EntityMetricBase[]
        """
        query = self._build_query(entity, counter_ids, instances, max_samples=1)
        query_specs = [query]
        return vim.PerformanceManager.QueryPerf(self.perf_manager, query_specs)

    def historical_stats(self, entity, counter_ids=None, instances=None, interval=7200, delay=4 * 3600):
        """Obtain historical statistics of given managed object.

        :param entity: a managed object
        :param counter_ids: keys of counters to query
        :param instances: which instances (e.g. network adapters, control wires) to query
        :param interval: interval of statistics to query
        :param delay: statistics query update, 4 hours observed for test server
        :rtype: vim.PerformanceManager.EntityMetricBase[]
        """
        start_time = self.service_instance.CurrentTime() - timedelta(seconds=interval + delay)
        end_time = self.service_instance.CurrentTime()
        query = self._build_query(entity, counter_ids, instances, interval, start_time, end_time)
        query_specs = [query]
        return vim.PerformanceManager.QueryPerf(self.perf_manager, query_specs)

    def latest_stats_by_specs(self, entity, counter_ids=None, instances=None):
        """Obtain the latest statistics of given managed object using query specifications.

        :param entity: a managed object
        :param counter_ids: keys of counters to query
        :param instances: which instances (e.g. network adapters, control wires) to query
        :rtype: pd.DataFrame
        """
        data = pd.DataFrame(columns=["key", "instance", "timestamp", "value", "unit", "name"])
        results = self.realtime_stats(entity, counter_ids, instances)
        for r in results:  # len(results) == len(query_specs), in this case the value is fixed 1
            sample_info, value = r.sampleInfo, r.value
            for v in value:  # in this case the value equals to the number of instances (including *)
                key, instance, val = v.id.counterId, v.id.instance, v.value
                for i in range(len(sample_info)):  # len(sample_info) == len(val), in this case the value is fixed 1
                    new_row = pd.DataFrame({"key": key, "instance": instance, "timestamp": sample_info[i].timestamp,
                                            "unit": self.all_counters[key].unitInfo.label, "value": val[i]}, index=[0])
                    data = data.append(new_row, ignore_index=True)
        data = data.sort_values(by='key').reset_index(drop=True)
        data['name'] = entity.name
        return data

    def latest_stats_all(self, entity):
        """Obtain the latest statistics (including all counter ids and instances) of given managed object.

        :param entity: a managed object
        :rtype: pd.DataFrame
        """
        counter_ids = self._get_available_counters(entity)
        return self.latest_stats_by_specs(entity, counter_ids, ["*"])

    def vm_data(self):
        """Output the latest statistics of all virtual machines in JSON-like form.
        
        :rtype: dict
        """
        data = {}
        for vm in self.vm_view:
            counter_ids = self._get_available_counters(vm)
            power_state = vm.runtime.powerState
            if power_state == "poweredOn":
                counters = {k: [] for k in counter_ids}
                # realtime stats
                results = self.realtime_stats(vm, counter_ids)
                if results:
                    sample_info, value = results[0].sampleInfo, results[0].value
                    timestamp = sample_info[0].timestamp
                    for v in value:
                        key, instance, val = v.id.counterId, v.id.instance, v.value[0]
                        unit = self.all_counters[key].unitInfo.label
                        description = self.all_counters[key].nameInfo.summary
                        new_data = {"instance": instance, "description": description, "value": f"{val}{unit}",
                                    "last_updated": f"{timestamp}"}
                        counters[key].append(new_data)
                # historical stats
                results = self.historical_stats(vm, [266, 267, 268, 269])
                if results:
                    sample_info, value = results[0].sampleInfo, results[0].value
                    timestamp = sample_info[0].timestamp
                    for v in value:
                        key, instance, val = v.id.counterId, v.id.instance, v.value[0]
                        unit = self.all_counters[key].unitInfo.label
                        description = self.all_counters[key].nameInfo.summary
                        new_data = {"instance": instance, "description": description, "value": f"{val}{unit}",
                                    "last_updated": f"{timestamp}"}
                        counters[key].append(new_data)
                # quick stats
                quick_stats = vm.summary.quickStats
                swapped, balloned = quick_stats.swappedMemory, quick_stats.balloonedMemory
                swapped_data = {"instance": "", "description": "The portion of memory that is granted to this VM from "
                                                               "the host's swap space", "value": f"{swapped}MB",
                                "last_updated": f"{self.service_instance.CurrentTime()}"}
                counters["quick-1"] = [swapped_data]
                balloned_data = {"instance": "", "description": "The size of the balloon driver in the VM. The host "
                                                                "will inflate the balloon driver to reclaim physical "
                                                                "memory from the VM", "value": f"{balloned}MB",
                                 "last_updated": f"{self.service_instance.CurrentTime()}"}
                counters["quick-2"] = [balloned_data]
                data[vm.name] = counters
        return data

    def host_data(self):
        """Output the latest statistics of all host systems in JSON-like form.

        :rtype: dict
        """
        data = {}
        for h in self.host_view:
            counter_ids = self._get_available_counters(h)
            power_state = h.runtime.powerState
            if power_state == "poweredOn":
                counters = {k: [] for k in counter_ids}
                # realtime stats
                results = self.realtime_stats(h, counter_ids)
                if results:
                    sample_info, value = results[0].sampleInfo, results[0].value
                    timestamp = sample_info[0].timestamp
                    for v in value:
                        key, instance, val = v.id.counterId, v.id.instance, v.value[0]
                        unit = self.all_counters[key].unitInfo.label
                        description = self.all_counters[key].nameInfo.summary
                        new_data = {"instance": instance, "description": description, "value": f"{val}{unit}",
                                    "last_updated": f"{timestamp}"}
                        counters[key].append(new_data)
                # historical stats
                results = self.historical_stats(h, [215, 216])
                if results:
                    sample_info, value = results[0].sampleInfo, results[0].value
                    timestamp = sample_info[0].timestamp
                    for v in value:
                        key, instance, val = v.id.counterId, v.id.instance, v.value[0]
                        unit = self.all_counters[key].unitInfo.label
                        description = self.all_counters[key].nameInfo.summary
                        new_data = {"instance": instance, "description": description, "value": f"{val}{unit}",
                                    "last_updated": f"{timestamp}"}
                        counters[key].append(new_data)
                    data[h.name] = counters
                # Note: no useful quick stats, at least for now
        return data

    def vm_memory_report(self):
        """Print virtual machine memory information based on quick stats.

        # vm memory pressure: https://developer.vmware.com/apis/358/vsphere/doc/vim.vm.Summary.QuickStats.html
        # memory release procedure: https://blog.csdn.net/weixin_42463871/article/details/117417406
        # VMWare best practice: https://virtual.51cto.com/art/201804/571507.htm

        :rtype: NoneType
        """
        for vm in self.vm_view:
            power_state = vm.runtime.powerState
            if power_state == "poweredOn":
                quick_stats = vm.summary.quickStats
                swapped, balloned = quick_stats.swappedMemory, quick_stats.balloonedMemory
                print(f"{vm.name}: {'Normal' if swapped == 0 and balloned == 0 else 'Warning'} (swapped {swapped} "
                      f"balloned {balloned})")
            else:
                print(f"{vm.name}: offline")

    async def vm_report(self):
        """Print virtual machine memory information every 20 seconds without termination.
        
        :rtype: NoneType
        """
        if self.refresh_rate == -1:
            warnings.warn("Realtime statistics not available")
        while True:
            print()
            print(datetime.now())
            self.vm_memory_report()
            print()
            await asyncio.sleep(20)

    def list_nodes(self):
        """Print all nodes in the server structure by tree.
        
        :rtype: NoneType
        """
        for i, t in enumerate(self.trees):
            print(f"(tree{i})", end=" ")
            print(", ".join([f"{n.value}: {n.id}" for n in self.nodes]))

    def query_id_status(self, id_):
        """Query the latest statistics for node with given id.
        
        :param id_: id of given node
        :rtype: pd.DataFrame
        """
        for t in self.trees:
            node = t.search(id_)
            if node:
                return self.latest_stats_all(node.value)
