// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __EXTERNAL_ISOLATOR_HPP__
#define __EXTERNAL_ISOLATOR_HPP__

#include <string>

#include <process/grpc.hpp>
#include <process/owned.hpp>

#include <stout/bytes.hpp>
#include <stout/duration.hpp>
#include <stout/hashmap.hpp>

#include "slave/flags.hpp"

#include "slave/containerizer/mesos/isolator.hpp"

namespace mesos {
namespace internal {
namespace slave {

// This isolator fans out to an external GRPC server which implements the
// isolator plugin service.
class ExternalPluginIsolatorProcess : public MesosIsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(const Flags& flags);

  virtual ~ExternalPluginIsolatorProcess();

  virtual bool supportsNesting();
  virtual bool supportsStandalone();

  virtual process::Future<Nothing> recover(
      const std::vector<mesos::slave::ContainerState>& states,
      const hashset<ContainerID>& orphans);

  virtual process::Future<Option<mesos::slave::ContainerLaunchInfo>> prepare(
      const ContainerID& containerId,
      const mesos::slave::ContainerConfig& containerConfig);

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  virtual process::Future<mesos::slave::ContainerLimitation> watch(
      const ContainerID& containerId);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  ExternalPluginIsolatorProcess(const Flags& flags);

  process::Future<Bytes> collect(
      const ContainerID& containerId,
      const std::string& path);

  void _collect(
      const ContainerID& containerId,
      const std::string& path,
      const process::Future<Bytes>& future);

  const Flags flags;

  bool _supportsNesting;

  // TODO(zhitao): Share one runtime across all plugins.
  process::grpc::client::Runtime runtime;
  std::unique_ptr<process::grpc::client::Connection> conn;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __EXTERNAL_ISOLATOR_HPP__
