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
#include <vector>

#include <process/collect.hpp>
#include <process/future.hpp>
#include <process/grpc.hpp>
#include <process/pid.hpp>

#include <stout/hashset.hpp>

#include <mesos/plugins/isolator/isolator.pb.h>
#include <mesos/plugins/isolator/isolator.grpc.pb.h>

#include "slave/containerizer/mesos/isolators/plugins/external_isolator.hpp"

using std::string;
using std::vector;

using process::Failure;
using process::Future;

using process::grpc::StatusError;

using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::ContainerLimitation;
using mesos::slave::ContainerState;
using mesos::slave::Isolator;

using namespace mesos::plugins::isolator;

namespace mesos {
namespace internal {
namespace slave {

Try<Isolator*> ExternalPluginIsolatorProcess::create(const Flags& flags)
{
  return new MesosIsolator(process::Owned<MesosIsolatorProcess>(
        new ExternalPluginIsolatorProcess(flags)));
}


ExternalPluginIsolatorProcess::ExternalPluginIsolatorProcess(
  const Flags& _flags)
  : ProcessBase(process::ID::generate("external-plugin-isolator")),
    flags(_flags),
    _supportsNesting(false),
    runtime(),
    conn(new process::grpc::client::Connection(
      flags.external_isolator_sockets.get()))
{
  InitializeRequest request;
  Future<Try<InitializeResponse, StatusError>> response = runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Initialize),
      std::move(request));

  // TODO(zhitao): Make this async.
  process::await(response);
  CHECK(response->isSome()) << response->error();
  _supportsNesting = response.get()->support_nested();
}


ExternalPluginIsolatorProcess::~ExternalPluginIsolatorProcess() {}


bool ExternalPluginIsolatorProcess::supportsNesting()
{
  return _supportsNesting;
}


bool ExternalPluginIsolatorProcess::supportsStandalone()
{
  return true;
}


Future<Nothing> ExternalPluginIsolatorProcess::recover(
    const vector<ContainerState>& states,
    const hashset<ContainerID>& orphans)
{
  RecoverRequest request;

  for (const auto& state : states) {
    request.add_container_states()->CopyFrom(state);
  }

  for (const auto& orphan : orphans) {
    request.add_orphans()->CopyFrom(orphan);
  }

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Recover),
      std::move(request))
    .then([](const Try<RecoverResponse, StatusError>& response)
          -> Future<Nothing> {
      if (response.isError()) {
        return Failure(response.error());
      }

      return Nothing();
    });
}


Future<Option<ContainerLaunchInfo>> ExternalPluginIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  PrepareRequest request;
  request.mutable_container_id()->CopyFrom(containerId);
  request.mutable_container_config()->CopyFrom(containerConfig);

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Prepare),
      std::move(request))
    .then([](const Try<PrepareResponse, StatusError>& response)
          -> Future<Option<ContainerLaunchInfo>> {
      if (response.isError()) {
        return Failure(response.error());
      }

      if (!response->has_launch_info()) {
        return None();
      }

      return response->launch_info();
    });
}


Future<Nothing> ExternalPluginIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  if (containerId.has_parent() && !_supportsNesting) {
    return Nothing();
  }

  IsolateRequest request;
  request.mutable_container_id()->CopyFrom(containerId);
  request.set_pid(static_cast<uint64_t>(pid));

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Isolate),
      std::move(request))
    .then([](const Try<IsolateResponse, StatusError>& response)
          -> Future<Nothing> {
      if (response.isError()) {
        return Failure(response.error());
      }

      return Nothing();
    });
}


Future<ContainerLimitation> ExternalPluginIsolatorProcess::watch(
    const ContainerID& containerId)
{
  // TODO(zhitao): Support limitation rather than a pending which means no
  // limit.
  return Future<ContainerLimitation>();
}


Future<Nothing> ExternalPluginIsolatorProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  UpdateRequest request;
  request.mutable_container_id()->CopyFrom(containerId);
  request.mutable_resources()->CopyFrom(resources);

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Update),
      std::move(request))
    .then([](const Try<UpdateResponse, StatusError>& response)
          -> Future<Nothing> {
      if (response.isError()) {
        return Failure(response.error());
      }

      return Nothing();
    });
}


Future<ResourceStatistics> ExternalPluginIsolatorProcess::usage(
    const ContainerID& containerId)
{
  UsageRequest request;
  request.mutable_container_id()->CopyFrom(containerId);

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Usage),
      std::move(request))
    .then([](const Try<UsageResponse, StatusError>& response)
          -> Future<ResourceStatistics> {
      if (response.isError()) {
        return Failure(response.error());
      }

      return response->statistics();
    });
}


Future<Nothing> ExternalPluginIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  CleanupRequest request;
  request.mutable_container_id()->CopyFrom(containerId);

  return runtime.call(
      *conn,
      GRPC_CLIENT_METHOD(IsolatorPlugin, Cleanup),
      std::move(request))
    .then([](const Try<CleanupResponse, StatusError>& response)
          -> Future<Nothing> {
       if (response.isError()) {
        return Failure(response.error());
      }

      return Nothing();
    });
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
